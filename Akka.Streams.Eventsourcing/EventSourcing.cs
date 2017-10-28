using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;

namespace Akka.Streams.Eventsourcing
{
    /// <summary>
    /// Event handler. Input is the current state and an event that has been written to an
    /// event log.Output is the updated state that is set to the current state by the
    /// [[EventSourcing]] driver.
    /// </summary>
    /// <typeparam name="TState"></typeparam>
    /// <typeparam name="TEvent"></typeparam>
    /// <param name="state"></param>
    /// <param name="e"></param>
    /// <returns></returns>
    public delegate TState EventHandler<TState, TEvent>(TState state, TEvent e);

    /// <summary>
    /// Request handler. Input is current state and a request, output is an instruction to emit events
    /// and/or a response:
    ///
    ///  - [[respond]] creates an immediate response which can be either the response to a ''query''
    /// or the failure response to a ''command'' whose validation failed, for example.
    ///
    ///  - [[emit]] returns a sequence of events to be written to an event log and a response factory
    /// to be called with the current state after all written events have been applied to it.
    /// </summary>
    /// <typeparam name="TState"></typeparam>
    /// <typeparam name="TEvent"></typeparam>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    /// <param name="state"></param>
    /// <param name="request"></param>
    /// <returns></returns>
    public delegate Emission<TState, TEvent, TResponse> RequestHandler<TState, TEvent, TRequest, TResponse>(TState state, TRequest request);

    public abstract class Emission<TState, TEvent, TResponse>
    {
        public static Emission<TState, TEvent, TResponse> Respond(TResponse response) => new Respond<TState, TEvent, TResponse>(response);
        public static Emission<TState, TEvent, TResponse> Emit(IReadOnlyCollection<TEvent> events, Func<TState, TResponse> responseFactory) =>
            new Emit<TState, TEvent, TResponse>(events, responseFactory);
    }

    public sealed class Respond<TState, TEvent, TResponse> : Emission<TState, TEvent, TResponse>
    {
        public TResponse Response { get; }

        public Respond(TResponse response)
        {
            Response = response;
        }
    }

    public sealed class Emit<TState, TEvent, TResponse> : Emission<TState, TEvent, TResponse>
    {
        public IReadOnlyCollection<TEvent> Events { get; }
        public Func<TState, TResponse> ResponseFactory { get; }

        public Emit(IReadOnlyCollection<TEvent> events, Func<TState, TResponse> responseFactory)
        {
            if (events.Count == 0) throw new ArgumentException("Cannot emit empty event collection", nameof(events));

            Events = events;
            ResponseFactory = responseFactory;
        }
    }

    public static class EventSourcing
    {
        /// <summary>
        /// Creates a router that routes to different <paramref name="processor"/>s based on input element key <typeparamref name="TKey"/>.
        /// A key is computed from input elements with the <paramref name="keySelector"/> function. Whenever a new key is
        /// encountered, a new key-specific processor is created with the <paramref name="processor"/> function.
        /// A processor processes all input elements of given key. Processor output elements
        /// are merged back into the main flow returned by this method.
        /// </summary>
        /// <param name="keySelector">computes a key from an input element</param>
        /// <param name="processor">key-specific processor factory</param>
        /// <param name="maxProcessors">maximum numbers of concurrent processors</param>
        /// <typeparam name="TIn">router and processor input type</typeparam>
        /// <typeparam name="TOut">router and processor output type</typeparam>
        /// <typeparam name="TKey">key type</typeparam>
        /// <typeparam name="TMat">processor materialized value type</typeparam>
        public static Flow<TIn, TOut, NotUsed> Router<TIn, TOut, TKey, TMat>(
            Func<TIn, TKey> keySelector,
            Func<TKey, Flow<TIn, TOut, TMat>> processor,
            int maxProcessors = int.MaxValue) =>
            (Flow<TIn, TOut, NotUsed>)Flow.Create<TIn>()
                .GroupBy(maxProcessors, keySelector)
                .PrefixAndTail(1)
                .MergeMany(maxProcessors, tuple =>
                {
                    (var h, var t) = tuple;
                    return Source.From(h).Concat(t).Via(processor(keySelector(h.First())));
                })
                .MergeSubstreams();

        /// <summary>
        /// Creates a bidi-flow that implements the driver for event sourcing logic defined by `requestHandler`
        /// `eventHandler`. The created event sourcing stage should be joined with an event log (i.e. a flow)
        /// for writing emitted events. Written events are delivered from the joined event log back to the stage:
        /// 
        ///  - After materialization, the stage's state is recovered with replayed events delivered by the joined
        ///    event log.
        ///  - On recovery completion (see [[Delivery]]) the stage is ready to accept requests if there is
        ///    downstream response and event demand.
        ///  - On receiving a command it calls the request handler and emits the returned events. The emitted events
        ///    are sent downstream to the joined event log.
        ///  - For each written event that is delivered from the event log back to the event sourcing stage, the
        ///    event handler is called with that event and the current state. The stage updates its current state
        ///    with the event handler result.
        ///  - After all emitted events (for a given command) have been applied, the response function, previously
        ///    created by the command handler, is called with the current state and the created response is emitted.
        ///  - After response emission, the stage is ready to accept the next request if there is downstream response
        ///    and event demand.
        /// </summary>
        /// @param emitterId Identifier used for [[Emitted.emitterId]].
        /// @param initial Initial state.
        /// @param requestHandler The stage's request handler.
        /// @param eventHandler The stage's event handler.
        /// @tparam S State type.
        /// @tparam E Event type.
        /// @tparam REQ Request type.
        /// @tparam RES Response type. 
        public static BidiFlow<TRequest, Emitted<TEvent>, Delivery<Durable<TEvent>>, TResponse, NotUsed> Create<TState, TEvent, TRequest, TResponse>(
            string emitterId,
            TState initialState,
            RequestHandler<TState, TEvent, TRequest, TResponse> requestHandler,
            EventHandler<TState, TEvent> eventHandler)
        {
            return BidiFlow.FromGraph(new EventSourcing<TState, TEvent, TRequest, TResponse>(emitterId, initialState, _ => requestHandler, _ => eventHandler));
        }

        /// <summary>
        /// Creates a bidi-flow that implements the driver for event sourcing logic returned by `requestHandlerProvider`
        /// and `eventHandlerProvider`. `requestHandlerProvider` is evaluated with current state for each received request,
        /// `eventHandlerProvider` is evaluated with current state for each written event. This can be used by applications
        /// to switch request and event handling logic as a function of current state. The created event sourcing stage
        /// should be joined with an event log (i.e. a flow) for writing emitted events. Written events are delivered
        /// from the joined event log back to the stage:
        /// 
        ///  - After materialization, the stage's state is recovered with replayed events delivered by the joined
        ///    event log.
        ///  - On recovery completion (see [[Delivery]]) the stage is ready to accept requests if there is
        ///    downstream response and event demand.
        ///  - On receiving a command it calls the request handler and emits the returned events. The emitted events
        ///    are sent downstream to the joined event log.
        ///  - For each written event that is delivered from the event log back to the event sourcing stage, the
        ///    event handler is called with that event and the current state. The stage updates its current state
        ///    with the event handler result.
        ///  - After all emitted events (for a given command) have been applied, the response function, previously
        ///    created by the command handler, is called with the current state and the created response is emitted.
        ///  - After response emission, the stage is ready to accept the next request if there is downstream response
        ///    and event demand.
        /// </summary>
        /// @param emitterId Identifier used for [[Emitted.emitterId]].
        /// @param initial Initial state.
        /// @param requestHandlerProvider The stage's request handler provider.
        /// @param eventHandlerProvider The stage's event handler provider.
        /// @tparam S State type.
        /// @tparam E Event type.
        /// @tparam REQ Request type.
        /// @tparam RES Response type.
        /// 
        public static BidiFlow<TRequest, Emitted<TEvent>, Delivery<Durable<TEvent>>, TResponse, NotUsed> Create<TState, TEvent, TRequest, TResponse>(
            string emitterId,
            TState initialState,
            Func<TState, RequestHandler<TState, TEvent, TRequest, TResponse>> requestHandlerProvider,
            Func<TState, EventHandler<TState, TEvent>> eventHandlerProvider)
        {
            return BidiFlow.FromGraph(new EventSourcing<TState, TEvent, TRequest, TResponse>(emitterId, initialState, requestHandlerProvider, eventHandlerProvider));
        }

        /// <summary>
        /// Creates a request handler result that contains an immediate response.
        /// </summary>
        public static Emission<TState, TEvent, TResponse> Respond<TState, TEvent, TResponse>(TResponse response) => new Respond<TState, TEvent, TResponse>(response);

        /// <summary>
        /// Create a request handler result that contains events to be written to an event log and a response
        /// factory to be called with the current state after all written events have been applied to it.
        /// </summary>
        public static Emission<TState, TEvent, TResponse> Emit<TState, TEvent, TResponse>(IReadOnlyCollection<TEvent> events, Func<TState, TResponse> responseFactory) => 
            new Emit<TState, TEvent, TResponse>(events, responseFactory);
    }

    internal sealed class EventSourcing<TState, TEvent, TRequest, TResponse> : GraphStage<BidiShape<TRequest, Emitted<TEvent>, Delivery<Durable<TEvent>>, TResponse>>
    {
        private readonly string emitterId;
        private readonly TState initialState;
        private readonly Func<TState, RequestHandler<TState, TEvent, TRequest, TResponse>> requestHandlerProvider;
        private readonly Func<TState, EventHandler<TState, TEvent>> eventHanlderProvider;

        public EventSourcing(string emitterId, TState initialState, Func<TState, RequestHandler<TState, TEvent, TRequest, TResponse>> requestHandlerProvider, Func<TState, EventHandler<TState, TEvent>> eventHanlderProvider)
        {
            this.emitterId = emitterId;
            this.initialState = initialState;
            this.requestHandlerProvider = requestHandlerProvider;
            this.eventHanlderProvider = eventHanlderProvider;

            Shape = new BidiShape<TRequest, Emitted<TEvent>, Delivery<Durable<TEvent>>, TResponse>(RequestIn, EventOut, EventIn, ResponseOut);
        }

        public Inlet<TRequest> RequestIn { get; } = new Inlet<TRequest>("EventSourcing.requestIn");
        public Outlet<Emitted<TEvent>> EventOut { get; } = new Outlet<Emitted<TEvent>>("EventSourcing.eventOut");
        public Inlet<Delivery<Durable<TEvent>>> EventIn { get; } = new Inlet<Delivery<Durable<TEvent>>>("EventSourcing.eventIn");
        public Outlet<TResponse> ResponseOut { get; } = new Outlet<TResponse>("EventSourcing.responseOut");
        public override BidiShape<TRequest, Emitted<TEvent>, Delivery<Durable<TEvent>>, TResponse> Shape { get; }
        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);

        #region internal classes

        private struct Roundtrip
        {
            public ImmutableHashSet<Guid> EmissionIds { get; }
            public Func<TState, TResponse> ResponseFactory { get; }

            public Roundtrip(ImmutableHashSet<Guid> emissionIds, Func<TState, TResponse> responseFactory)
            {
                this.EmissionIds = emissionIds;
                this.ResponseFactory = responseFactory;
            }

            public Roundtrip Delivered(Guid emissionId) => new Roundtrip(EmissionIds.Remove(emissionId), ResponseFactory);
        }

        private sealed class Logic : GraphStageLogic
        {
            private EventSourcing<TState, TEvent, TRequest, TResponse> stage;

            private bool requestUpstreamFinished = false;
            private bool recovered = false;
            private Roundtrip? roundtrip = null;
            private TState state;

            public Logic(EventSourcing<TState, TEvent, TRequest, TResponse> stage) : base(stage.Shape)
            {
                this.stage = stage;
                this.state = stage.initialState;

                SetHandler(stage.RequestIn, onPush: () =>
                {
                    var emission = stage.requestHandlerProvider(state)(state, Grab(stage.RequestIn));
                    switch (emission)
                    {
                        case Respond<TState, TEvent, TResponse> respond:
                            Push(stage.ResponseOut, respond.Response);
                            TryPullRequestIn();
                            break;
                        case Emit<TState, TEvent, TResponse> emit:
                            var emitted = emit.Events.Select(e => new Emitted<TEvent>(e, stage.emitterId));
                            roundtrip = new Roundtrip(emitted.Select(e => e.EmissionId).ToImmutableHashSet(), emit.ResponseFactory);
                            break;
                    }
                }, onUpstreamFinish: () =>
                {
                    if (!roundtrip.HasValue) CompleteStage();
                    else requestUpstreamFinished = true;
                });

                SetHandler(stage.EventIn, onPush: () =>
                {
                    switch (Grab(stage.EventIn))
                    {
                        case Delivered<Durable<TEvent>> delivered:
                            state = stage.eventHanlderProvider(state)(state, delivered.Data.Event);
                            if (roundtrip.HasValue)
                            {
                                var r = roundtrip.Value.Delivered(delivered.Data.EmissionId);
                                if (r.EmissionIds.IsEmpty)
                                {
                                    Push(stage.ResponseOut, r.ResponseFactory(state));

                                    if (requestUpstreamFinished) CompleteStage();
                                    else TryPullRequestIn();

                                    roundtrip = null;
                                }
                                else
                                {
                                    roundtrip = r;
                                }
                            }
                            break;
                        case Recovered<Durable<TEvent>> _:
                            recovered = true;
                            TryPullRequestIn();
                            break;
                    }
                });

                SetHandler(stage.EventOut, onPull: TryPullRequestIn);

                SetHandler(stage.ResponseOut, onPull: TryPullRequestIn);
            }

            public override void PreStart()
            {
                base.PreStart();
                TryPullEventIn();
            }

            private void TryPullEventIn()
            {
                if (!requestUpstreamFinished) Pull(stage.EventIn);
            }

            private void TryPullRequestIn()
            {
                if (!requestUpstreamFinished && recovered && roundtrip == null && IsAvailable(stage.ResponseOut) && !HasBeenPulled(stage.RequestIn))
                    Pull(stage.RequestIn);
            }
        }

        #endregion
    }
}