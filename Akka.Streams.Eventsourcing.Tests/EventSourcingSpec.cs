using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Streams.Eventsourcing.Log;
using Xunit.Abstractions;
using static Akka.Streams.Eventsourcing.EventSourcing;
using Akka.Streams.Dsl;
using FluentAssertions;
using Xunit;

namespace Akka.Streams.Eventsourcing.Tests
{
    public interface IRequest { }
    public interface IEvent { }

    public sealed class GetState : IRequest
    {
        public static readonly GetState Instance = new GetState();
        private GetState() { }
    }

    public sealed class Increment : IRequest
    {
        public Increment(int delta)
        {
            Delta = delta;
        }

        public int Delta { get; }
    }

    public sealed class ClearIfEqualTo : IRequest
    {
        public ClearIfEqualTo(int value)
        {
            Value = value;
        }

        public int Value { get; }
    }

    public sealed class Response
    {
        public Response(int state)
        {
            State = state;
        }

        public int State { get; }
    }

    public sealed class Incremented : IEvent
    {
        public Incremented(int delta)
        {
            Delta = delta;
        }

        public int Delta { get; }
    }

    public sealed class Cleared : IEvent
    {
        public static readonly Cleared Instance = new Cleared();
        private Cleared() { }
    }

    public class EventSourcingSpec : StreamSpec
    {
        private readonly PersistenceEventLog _eventLog;

        public EventSourcingSpec(ITestOutputHelper output) : base(output)
        {
            _eventLog = new PersistenceEventLog(Sys, "akka.persistence.journal.inmem");
        }

        private Flow<IRequest, Response, NotUsed> Processor(IEnumerable<Emitted<IEvent>> replay = null) =>
            EventSourcing.Create(EmitterId, 0, RequestHandler, EventHandler).Join(TestEventLog(replay));

        private Flow<IRequest, Response, NotUsed> PersistenceProcessor(string pid) =>
            EventSourcing.Create(EmitterId, 0, RequestHandler, EventHandler).Join(_eventLog.Flow<IEvent>(pid));

        private Flow<Emitted<T>, Delivery<Durable<T>>, NotUsed> TestEventLog<T>(IEnumerable<Emitted<T>> emitted = null) =>
            Flow.Create<Emitted<T>>()
                .ZipWithIndex().Select(x => x.Item1.ToDurable(x.Item2))
                .Select(x => new Delivered<Durable<T>>(x) as Delivery<Durable<T>>)
                .Prepend(Source.Single(Recovered<Durable<T>>.Instance as Delivery<Durable<T>>))
                .Prepend(Source.From(Durables(emitted ?? new Emitted<T>[0])).Select(x => new Delivered<Durable<T>>(x) as Delivery<Durable<T>>));

        #region abstract
        
        [Fact]
        public void EventSourcing_stage_when_joined_with_event_log_must_consume_commands_and_produce_responses()
        {
            var commands = new[] { 1, -4, 7 }.Select(x => new Increment(x) as IRequest).ToArray();
            var expected = new[] { 1, -3, 4 }.Select(x => new Response(x)).ToArray();

            Source.From(commands).Via(Processor()).RunWith(Sink.Seq<Response>(), Materializer).Result.Should().Equal(expected);
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_event_log_must_consume_queries_and_produce_responses()
        {
            var commands = new IRequest[] { new Increment(1), GetState.Instance, new Increment(7) };
            var expected = new[] { new Response(1), new Response(1), new Response(8), };

            Source.From(commands).Via(Processor()).RunWith(Sink.Seq<Response>(), Materializer).Result.Should().Equal(expected);
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_non_empty_event_log_must_first_recover_state_then_consume_comands_and_produce_responses()
        {
            var commands = new IRequest[] { new Increment(-4), new Increment(7) };
            var expected = new[] { new Response(-3), new Response(4), };

            Source.From(commands).Via(Processor(new[] { new Emitted<IEvent>(new Incremented(1), EmitterId) })).RunWith(Sink.Seq<Response>(), Materializer).Result.Should().Equal(expected);
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_non_empty_event_log_must_first_recover_state_then_consume_state_dependent_commands_with_correct_state()
        {
            Source.Single(new ClearIfEqualTo(5) as IRequest)
                .Via(Processor(new[] { new Emitted<IEvent>(new Incremented(5), EmitterId) }))
                .RunWith(Sink.Seq<Response>(), Materializer)
                .Result.Should().Equal(new[] { new Response(0) });
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_non_empty_event_log_must_first_recover_state_then_consume_comand_followed_by_state_dependent_command_with_correct_state()
        {
            Source.From(new IRequest[] { new Increment(0), new ClearIfEqualTo(5) })
                .Via(Processor(new[] { new Emitted<IEvent>(new Incremented(5), EmitterId) }))
                .RunWith(Sink.Seq<Response>(), Materializer)
                .Result.Should().Equal(new[] { new Response(5), new Response(0) });
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_non_empty_event_log_must_first_recover_state_then_consume_comand_and_produce_response()
        {
            Source.From(new IRequest[] { new Increment(2) })
                .Via(Processor(new[] { new Emitted<IEvent>(new Incremented(1), EmitterId) }))
                .RunWith(Sink.Seq<Response>(), Materializer)
                .Result.Should().Equal(new[] { new Response(3) });
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_non_empty_event_log_must_first_recover_state_then_consume_query_and_produce_response()
        {
            Source.From(new IRequest[] { GetState.Instance,  })
                .Via(Processor(new[] { new Emitted<IEvent>(new Incremented(1), EmitterId) }))
                .RunWith(Sink.Seq<Response>(), Materializer)
                .Result.Should().Equal(new[] { new Response(1) });
        }

        #endregion

        #region Persistence


        [Fact]
        public void EventSourcing_stage_when_joined_with_Persistence_event_log_must_consume_commands_and_produce_responses()
        {
            const string pid = "p-1";
            var commands = new[] { 1, -4, 7 }.Select(x => new Increment(x) as IRequest).ToArray();
            var expected = new[] { 1, -3, 4 }.Select(x => new Response(x)).ToArray();

            Source.From(commands)
                .Via(PersistenceProcessor(pid))
                .RunWith(Sink.Seq<Response>(), Materializer)
                .Result.Should().Equal(expected);
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_Persistence_event_log_must_first_recover_state_then_consume_comands_and_produce_responses()
        {
            const string pid = "p-2";
            Source.Single(new Emitted<IEvent>(new Incremented(1), EmitterId)).RunWith(_eventLog.Sink<IEvent>(pid), Materializer).Wait(Timeout);


            var commands = new IRequest[] { new Increment(-4), new Increment(7) };
            var expected = new[] { new Response(-3), new Response(4), };

            Source.From(commands).Via(PersistenceProcessor(pid)).RunWith(Sink.Seq<Response>(), Materializer).Result.Should().Equal(expected);
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_Persistence_event_log_must_first_recover_state_then_consume_state_dependent_commands_with_correct_state()
        {
            const string pid = "p-3";
            Source.Single(new Emitted<IEvent>(new Incremented(5), EmitterId)).RunWith(_eventLog.Sink<IEvent>(pid), Materializer).Wait(Timeout);

            Source.Single(new ClearIfEqualTo(5) as IRequest)
                .Via(PersistenceProcessor(pid))
                .RunWith(Sink.Seq<Response>(), Materializer)
                .Result.Should().Equal(new[] { new Response(0) });
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_Persistence_event_log_must_first_recover_state_then_consume_comand_followed_by_state_dependent_command_with_correct_state()
        {
            const string pid = "p-4";
            Source.Single(new Emitted<IEvent>(new Incremented(5), EmitterId)).RunWith(_eventLog.Sink<IEvent>(pid), Materializer).Wait(Timeout);

            Source.From(new IRequest[] { new Increment(0), new ClearIfEqualTo(5) })
                .Via(PersistenceProcessor(pid))
                .RunWith(Sink.Seq<Response>(), Materializer)
                .Result.Should().Equal(new[] { new Response(5), new Response(0) });
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_Persistence_event_log_must_first_recover_state_then_consume_comand_and_produce_response()
        {
            const string pid = "p-5";
            Source.Single(new Emitted<IEvent>(new Incremented(1), EmitterId)).RunWith(_eventLog.Sink<IEvent>(pid), Materializer).Wait(Timeout);

            Source.From(new IRequest[] { new Increment(2) })
                .Via(PersistenceProcessor(pid))
                .RunWith(Sink.Seq<Response>(), Materializer)
                .Result.Should().Equal(new[] { new Response(3) });
        }

        [Fact]
        public void EventSourcing_stage_when_joined_with_Persistence_event_log_must_first_recover_state_then_consume_query_and_produce_response()
        {
            const string pid = "p-5";
            Source.Single(new Emitted<IEvent>(new Incremented(1), EmitterId)).RunWith(_eventLog.Sink<IEvent>(pid), Materializer).Wait(Timeout);

            Source.From(new IRequest[] { GetState.Instance, })
                .Via(PersistenceProcessor(pid))
                .RunWith(Sink.Seq<Response>(), Materializer)
                .Result.Should().Equal(new[] { new Response(1) });
        }

        #endregion
    }
}