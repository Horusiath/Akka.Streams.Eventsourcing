using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using System.Linq;

namespace Akka.Streams.Eventsourcing.Log
{
    public class Journal
    {
        private readonly ActorSystem _system;
        private readonly string _journalId;
        private IActorRef _journalRef;

        public Journal(ActorSystem system, string journalId)
        {
            _system = system;
            _journalId = journalId;

            var persistence = Persistence.Persistence.Instance.Apply(system);
            _journalRef = persistence.JournalFor(journalId);
        }

        public Flow<IPersistentRepresentation, Delivery<IPersistentRepresentation>, NotUsed> EventLog(string persistenceId) =>
            Flow.Create<IPersistentRepresentation>()
                .Batch(10, ImmutableList.Create, (batch, p) => batch.Add(p))
                .Via(Flow.FromGraph(new EventLog(_system, _journalRef, persistenceId)));

        public Source<Delivery<IPersistentRepresentation>, NotUsed> EventSource(string persistenceId) =>
            Source.Single(ImmutableList<IPersistentRepresentation>.Empty)
                .Via(Flow.FromGraph(new EventLog(_system, _journalRef, persistenceId)));

        public Sink<IPersistentRepresentation, Task> EventSink(string persistenceId) =>
            EventLog(persistenceId)
            .ToMaterialized(Sink.Ignore<Delivery<IPersistentRepresentation>>(), Keep.Right);
    }

    public class EventLog : GraphStage<FlowShape<ImmutableList<IPersistentRepresentation>, Delivery<IPersistentRepresentation>>>
    {
        private readonly IActorRefFactory factory;
        private readonly IActorRef journalRef;
        private readonly string persistenceId;

        private readonly EventWriter writer;
        private readonly EventReplayer replayer;

        public EventLog(IActorRefFactory factory, IActorRef journalRef, string persistenceId)
        {
            this.factory = factory;
            this.journalRef = journalRef;
            this.persistenceId = persistenceId;

            this.writer = new EventWriter(factory, journalRef, persistenceId);
            this.replayer = new EventReplayer(factory, journalRef, persistenceId);

            Shape = new FlowShape<ImmutableList<IPersistentRepresentation>, Delivery<IPersistentRepresentation>>(Inlet, Outlet);
        }

        public Inlet<ImmutableList<IPersistentRepresentation>> Inlet { get; } = new Inlet<ImmutableList<IPersistentRepresentation>>("EventLogStage.in");
        public Outlet<Delivery<IPersistentRepresentation>> Outlet { get; } = new Outlet<Delivery<IPersistentRepresentation>>("EventLogStage.out");
        public override FlowShape<ImmutableList<IPersistentRepresentation>, Delivery<IPersistentRepresentation>> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);

        #region logic

        private sealed class Logic : GraphStageLogic
        {
            private readonly EventLog eventLog;
            private readonly Action<EventWriter.Written> writeSuccessCallback;
            private readonly Action<EventReplayer.Replayed> replaySuccessCallback;
            private readonly Action<Exception> failureCallback;

            private bool completed = false;
            private bool writing = false;
            private bool replaying = true;
            private long currentSeqNr = 0L;

            public Logic(EventLog eventLog) : base(eventLog.Shape)
            {
                this.eventLog = eventLog;
                this.writeSuccessCallback = GetAsyncCallback<EventWriter.Written>(OnWriteSuccess);
                this.replaySuccessCallback = GetAsyncCallback<EventReplayer.Replayed>(OnReplaySuccess);
                this.failureCallback = GetAsyncCallback<Exception>(FailStage);

                SetHandler(eventLog.Inlet, onPush: () =>
                {
                    writing = true;
                    var events = Grab(eventLog.Inlet)
                        .Select(x => x.Update(currentSeqNr++, x.PersistenceId, x.IsDeleted, x.Sender, x.WriterGuid))
                        .ToImmutableList();
                    eventLog.writer
                        .WriteEvents(currentSeqNr + 1, events)
                        .ContinueWith(t =>
                        {
                            if (t.IsCanceled || t.IsFaulted)
                                failureCallback(t.Exception);
                            else
                                writeSuccessCallback(t.Result);
                        });
                }, onUpstreamFinish: () =>
                {
                    if (writing) completed = true;
                    else CompleteStage();
                });

                SetHandler(eventLog.Outlet, onPull: () =>
                {
                    if (replaying)
                    {
                        eventLog.replayer
                            .ReplayEvents(currentSeqNr + 1, 100)
                            .ContinueWith(t =>
                            {
                                if (t.IsCanceled || t.IsFaulted)
                                    failureCallback(t.Exception);
                                else
                                    replaySuccessCallback(t.Result);
                            });
                    }
                    else Pull(eventLog.Inlet);
                });
            }

            private void OnReplaySuccess(EventReplayer.Replayed replayed)
            {
                currentSeqNr = replayed.LastSeqNr;
                EmitMultiple(eventLog.Outlet, replayed.Events.Select(x => new Delivered<IPersistentRepresentation>(x)));
                if (currentSeqNr == replayed.CurrentSeqNr)
                {
                    replaying = false;
                    Emit(eventLog.Outlet, Recovered<IPersistentRepresentation>.Instance);
                }
            }

            private void OnWriteSuccess(EventWriter.Written written)
            {
                writing = false;
                EmitMultiple(eventLog.Outlet, written.Events.Select(x => new Delivered<IPersistentRepresentation>(x)));
                if (completed) CompleteStage();
            }
        }

        #endregion
    }

    internal sealed class EventWriter
    {
        #region internal classes

        public sealed class Write
        {
            public ImmutableList<IPersistentRepresentation> Events { get; }
            public long From { get; }

            public Write(ImmutableList<IPersistentRepresentation> events, long from)
            {
                Events = events;
                From = from;
            }
        }

        public sealed class Written
        {
            public IEnumerable<IPersistentRepresentation> Events { get; }

            public Written(IEnumerable<IPersistentRepresentation> events)
            {
                Events = events;
            }
        }

        #endregion

        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        private readonly IActorRefFactory factory;
        private readonly IActorRef journalRef;
        private readonly string persistenceId;

        public EventWriter(IActorRefFactory factory, IActorRef journalRef, string persistenceId)
        {
            this.factory = factory;
            this.journalRef = journalRef;
            this.persistenceId = persistenceId;
        }

        public Task<Written> WriteEvents(long from, IImmutableList<IPersistentRepresentation> events) => factory
                .ActorOf(Props.Create(() => new EventWriterActor(persistenceId, journalRef)))
                .Ask<Written>(new Write(events.ToImmutableList(), from), Timeout);
    }

    internal sealed class EventWriterActor : ActorBase
    {
        private readonly string persistenceId;
        private readonly IActorRef journalRef;

        private int written = 0;
        private ImmutableList<IPersistentRepresentation> events;
        private IActorRef sender;

        public EventWriterActor(string persistenceId, IActorRef journalRef)
        {
            this.persistenceId = persistenceId;
            this.journalRef = journalRef;
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case EventWriter.Write write:
                    if (write.Events.Count == 0)
                    {
                        Sender.Tell(new EventWriter.Written(new IPersistentRepresentation[0]));
                        Context.Stop(Self);
                    }
                    else
                    {
                        sender = Sender;
                        events = write.Events;
                        journalRef.Tell(new WriteMessages(new IPersistentEnvelope[] { new AtomicWrite(events) }, Self, 0));
                    }
                    return true;
                case WriteMessagesFailed fail:
                    sender.Tell(new Status.Failure(fail.Cause));
                    Context.Stop(Self);
                    return true;
                case WriteMessagesSuccessful _:
                    sender.Tell(new EventWriter.Written(events));
                    return true;
                case WriteMessageSuccess _:
                    written++;
                    if (written == events.Count) Context.Stop(Self);
                    return true;
                default: return false;
            }
        }
    }

    internal sealed class EventReplayer
    {
        #region internal classes

        public sealed class Replay
        {
            public long From { get; }
            public long Max { get; }

            public Replay(long @from, long max)
            {
                From = @from;
                Max = max;
            }
        }

        public sealed class Replayed
        {
            public ImmutableList<IPersistentRepresentation> Events { get; }
            public long CurrentSeqNr { get; }

            public Replayed(ImmutableList<IPersistentRepresentation> events, long currentSeqNr)
            {
                Events = events;
                CurrentSeqNr = currentSeqNr;
            }

            public long LastSeqNr => Events.Count == 0 ? CurrentSeqNr : Events.Last().SequenceNr;
        }

        #endregion

        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        private readonly IActorRefFactory factory;
        private readonly IActorRef journalRef;
        private readonly string persistenceId;

        public EventReplayer(IActorRefFactory factory, IActorRef journalRef, string persistenceId)
        {
            this.factory = factory;
            this.journalRef = journalRef;
            this.persistenceId = persistenceId;
        }

        public Task<Replayed> ReplayEvents(long from, long max) => factory
            .ActorOf(Props.Create(() => new EventReplayerActor(persistenceId, journalRef)))
            .Ask<Replayed>(new Replay(from, max), Timeout);
    }

    internal sealed class EventReplayerActor : ActorBase
    {
        private readonly string persistenceId;
        private readonly IActorRef journalRef;
        private readonly ImmutableList<IPersistentRepresentation>.Builder builder = ImmutableList<IPersistentRepresentation>.Empty.ToBuilder();
        private IActorRef sender;

        public EventReplayerActor(string persistenceId, IActorRef journalRef)
        {
            this.persistenceId = persistenceId;
            this.journalRef = journalRef;
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case EventReplayer.Replay replay:
                    sender = Sender;
                    journalRef.Tell(new ReplayMessages(replay.From, long.MaxValue, replay.Max, persistenceId, Self));
                    return true;
                case ReplayedMessage replayed:
                    builder.Add(replayed.Persistent);
                    return true;
                case ReplayMessagesFailure fail:
                    sender.Tell(new Status.Failure(fail.Cause));
                    Context.Stop(Self);
                    return true;
                case RecoverySuccess success:
                    sender.Tell(new EventReplayer.Replayed(builder.ToImmutable(), success.HighestSequenceNr));
                    return true;
                default: return false;
            }
        }
    }
}