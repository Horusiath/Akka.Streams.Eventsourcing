using System;
using System.Collections.Generic;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Xunit.Abstractions;

namespace Akka.Streams.Eventsourcing.Tests
{
    public abstract class StreamSpec : Akka.TestKit.Xunit.TestKit
    {
        protected static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
        protected ActorMaterializer Materializer;
        protected const string EmitterId = "emitter";

        protected static RequestHandler<int, IEvent, IRequest, Response> RequestHandler;
        protected static EventHandler<int, IEvent> EventHandler;

        static StreamSpec()
        {
            RequestHandler = (state, request) =>
            {
                switch (request)
                {
                    case GetState _: return new Respond<int, IEvent, Response>(new Response(state));
                    case Increment i: return new Emit<int, IEvent, Response>(new[] { new Incremented(i.Delta) }, x => new Response(x));
                    case ClearIfEqualTo c:
                        return state == c.Value
                            ? new Emit<int, IEvent, Response>(new[] { Cleared.Instance }, x => new Response(x)) as Emission<int, IEvent, Response>
                            : new Respond<int, IEvent, Response>(new Response(state));
                    default: throw new NotSupportedException($"Unknown request {request}");
                }
            };
            EventHandler = (state, e) =>
            {
                switch (e)
                {
                    case Incremented i: return state + i.Delta;
                    case Cleared _: return 0;
                    default: throw new NotSupportedException($"Unknown request {e}");
                }
            };
        }

        protected StreamSpec(ITestOutputHelper output) : base(output: output)
        {
            Materializer = Sys.Materializer();
        }

        protected override void AfterAll()
        {
            Materializer.Dispose();
            base.AfterAll();
        }

        protected Tuple<TestPublisher.Probe<TIn>, TestSubscriber.Probe<TOut>> Probes<TIn, TOut, TMat>(Flow<TIn, TOut, TMat> flow) =>
            this.SourceProbe<TIn>().ViaMaterialized(flow, Keep.Left).ToMaterialized(this.SinkProbe<TOut>(), Keep.Both).Run(Materializer);

        protected IEnumerable<Durable<T>> Durables<T>(IEnumerable<Emitted<T>> emitted, int offset = 0)
        {
            var i = 0;
            foreach (var e in emitted)
            {
                yield return e.ToDurable(i + offset);
                i++;
            }
        }
    }
}