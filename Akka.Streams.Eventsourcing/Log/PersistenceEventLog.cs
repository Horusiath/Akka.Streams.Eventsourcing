using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence;
using Akka.Streams.Dsl;

namespace Akka.Streams.Eventsourcing.Log
{
    /// <summary>
    /// Akka Stream API for events logs managed by an Akka Persistence journal.
    /// An individual event log is identified by `persistenceId`.
    /// </summary>
    /// @param journalId Id of the Akka Persistence journal. 
    public class PersistenceEventLog
    {
        private readonly Journal journal;

        public PersistenceEventLog(ActorSystem system, string journalId)
        {
            this.journal = new Journal(system, journalId);
        }

        /// <summary>
        /// Creates a flow representing the event log identified by <paramref name="persistenceId"/>.
        /// Input events are written to the journal and emitted as output events
        /// after successful write. Before input events are requested from upstream
        /// the flow emits events that have been previously written to the journal
        /// (recovery phase).
        /// 
        /// During recovery, events are emitted as <see cref="Delivered{T}"/> messages, recovery completion
        /// is signaled as <see cref="Recovered{T}"/> message, both subtypes of <see cref="Delivery{T}"/> After recovery,
        /// events are again emitted as <see cref="Delivered{T}"/> messages.
        /// 
        /// It is the application's responsibility to ensure that there is only a
        /// single materialized instance with given `persistenceId` writing to the
        /// journal.
        /// </summary>
        /// <param name="persistenceId">persistence id of the event log</param>
        /// <typeparam name="T">A event type. Only events that are instances of given type are emitted by the flow.</typeparam>
        public Flow<Emitted<T>, Delivery<Durable<T>>, NotUsed> Flow<T>(string persistenceId) =>
            AkkaPersistenceCodec<T>.Create(persistenceId).Join(journal.EventLog(persistenceId));

        /// <summary>
        /// Creates a source that replays events of the event log identified by `persistenceId`.
        /// The source completes when the end of the log has been reached.
        /// 
        /// During recovery, events are emitted as <see cref="Delivered{T}"/> messages, recovery completion
        /// is signaled as <see cref="Recovered{T}"/> message, both subtypes of <see cref="Delivery{T}"/>.
        /// </summary>
        /// <param name="persistenceId">persistence id of the event log.</param>
        /// <typeparam name="T">A event type. Only events that are instances of given type are emitted by the source.</typeparam>
        public Source<Delivery<Durable<T>>, NotUsed> Source<T>(string persistenceId) =>
            journal.EventSource(persistenceId).Via(AkkaPersistenceCodec<T>.Decoder);

        /// <summary>
        /// Creates a sink that writes events to the event log identified by <paramref name="persitenceId"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="persitenceId">persistence id of the event log</param>
        /// <returns></returns>
        public Sink<Emitted<T>, Task> Sink<T>(string persitenceId) =>
            AkkaPersistenceCodec<T>.Encoder(persitenceId).ToMaterialized(journal.EventSink(persitenceId), Keep.Right);
    }

    internal static class AkkaPersistenceCodec<T>
    {
        public static BidiFlow<Emitted<T>, IPersistentRepresentation, Delivery<IPersistentRepresentation>, Delivery<Durable<T>>, NotUsed> Create(string persistenceId) => 
            BidiFlow.FromFlows(Encoder(persistenceId), Decoder);

        /// <summary>
        /// Decodes <see cref="IPersistentRepresentation"/> event as <see cref="Durable{T}"/>
        /// </summary>
        public static Flow<Delivery<IPersistentRepresentation>, Delivery<Durable<T>>, NotUsed> Decoder { get; } =
            Flow.Create<Delivery<IPersistentRepresentation>>()
                .Collect(delivery =>
                {
                    var delivered = delivery as Delivered<IPersistentRepresentation>;
                    if (delivered != null)
                    {
                        var e = delivered.Data.Payload as Emitted<T>;
                        if (e != null)
                            return (Delivery<Durable<T>>)new Delivered<Durable<T>>(new Durable<T>(
                                @event: e.Event,
                                emitterId: e.EmitterId,
                                sequenceNr: delivered.Data.SequenceNr,
                                emissionId: e.EmissionId));
                        else return null;
                    }
                    else return (delivery as Recovered<Durable<T>>);
                });

        /// <summary>
        /// Encodes an <see cref="Emitted{T}"/> event as <see cref="IPersistentRepresentation"/>.
        /// </summary>
        /// <param name="persistenceId"></param>
        /// <returns></returns>
        public static Flow<Emitted<T>, IPersistentRepresentation, NotUsed> Encoder(string persistenceId) => 
            Flow.Create<Emitted<T>>().Select(identified => (IPersistentRepresentation)new Persistent(
                payload: identified,
                sequenceNr: -1L,
                persistenceId: persistenceId,
                manifest: string.Empty,
                isDeleted: false,
                sender: null,
                writerGuid: string.Empty));
    }
}