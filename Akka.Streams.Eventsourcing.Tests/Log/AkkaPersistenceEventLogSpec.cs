using System;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Streams.Eventsourcing.Log;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Eventsourcing.Tests.Log
{
    public class AkkaPersistenceEventLogSpec : StreamSpec
    {
        private readonly PersistenceEventLog _eventLog;

        public AkkaPersistenceEventLogSpec(ITestOutputHelper output) : base(output: output)
        {
            _eventLog = new PersistenceEventLog(Sys, "akka.persistence.journal.inmem");
        }

        [Fact]
        public void Akka_Persistence_event_log_must_provide_a_sink_for_writing_events_and_source_for_delivering_replayed_events()
        {
            var pid = "1";
            var events = new[] {"a", "b", "c"}.Select(x => new Emitted<string>(x, EmitterId)).ToArray();
            var expected = Durables(events, offset: 1)
                .Select(x => (Delivery<Durable<string>>)new Delivered<Durable<string>>(x))
                .ToImmutableList()
                .Add(Recovered<Durable<string>>.Instance);

            Source.From(events).RunWith(_eventLog.Sink<string>(pid), Materializer).Wait(Timeout).Should().BeTrue();
            var t = _eventLog.Source<string>(pid).RunWith(Sink.Seq<Delivery<Durable<string>>>(), Materializer);
            t.Wait(Timeout).Should().BeTrue();
            t.Result.ShouldAllBeEquivalentTo(expected);
        }

        [Fact]
        public void Akka_Persistence_event_log_must_provide_a_flow_with_an_input_port_for_writing_events_and_and_output_port_for_delivering_replayed_and_live_events()
        {
            var pid = "2";
            var events1 = new[] { "a", "b", "c" }.Select(x => new Emitted<string>(x, EmitterId)).ToArray();
            var events2 = new[] { "d", "e", "f" }.Select(x => new Emitted<string>(x, EmitterId)).ToArray();
            var expected = Durables(events1, offset: 1)
                .Select(x => (Delivery<Durable<string>>)new Delivered<Durable<string>>(x))
                .ToImmutableList()
                .Add(Recovered<Durable<string>>.Instance)
                .AddRange(Durables(events2, offset: 4)
                    .Select(x => (Delivery<Durable<string>>)new Delivered<Durable<string>>(x)));

            Source.From(events1).RunWith(_eventLog.Sink<string>(pid), Materializer).Wait(Timeout).Should().BeTrue();
            var t = Source.From(events2).Via(_eventLog.Flow<string>(pid)).RunWith(Sink.Seq<Delivery<Durable<string>>>(), Materializer);
            t.Wait(Timeout).Should().BeTrue();
            t.Result.ShouldAllBeEquivalentTo(expected);
        }

        [Fact]
        public void Akka_Persistence_event_log_must_provide_a_source_that_only_delivers_events_to_compatible_types()
        {
            var pid = "3";
            var events = new object[] { "a", "b", 1, 2 }.Select(x => new Emitted<object>(x, EmitterId)).ToArray();
            var expected = Durables(events, offset: 1).Skip(2)
                .Select(x => (Delivery<Durable<object>>)new Delivered<Durable<object>>(x))
                .ToImmutableList()
                .Add(Recovered<Durable<object>>.Instance);

            Source.From(events).RunWith(_eventLog.Sink<object>(pid), Materializer).Wait(Timeout).Should().BeTrue();
            var t = _eventLog.Source<int>(pid).RunWith(Sink.Seq<Delivery<Durable<int>>>(), Materializer);
            t.Wait(Timeout).Should().BeTrue();
            t.Result.ShouldAllBeEquivalentTo(expected);
        }
    }
}