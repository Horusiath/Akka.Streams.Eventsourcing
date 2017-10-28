using Akka.Streams.Dsl;
using Akka.Streams.Eventsourcing.Log;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Eventsourcing.Tests
{
    public class EventCollaborationSpec : StreamSpec
    {
        public const string Pid = "p-1";
        public const string EmitterId1 = "processor1";
        public const string EmitterId2 = "processor2";

        private readonly PersistenceEventLog _eventLog;

        public EventCollaborationSpec(ITestOutputHelper output) : base(output)
        {
            _eventLog = new PersistenceEventLog(Sys, "akka.persistence.journal.inmem");
        }

        private Flow<IRequest, Response, NotUsed> Processor(string emitterId) =>
            EventSourcing.Create(emitterId, 0, RequestHandler, EventHandler).Join(_eventLog.Flow<IEvent>(emitterId));

        [Fact]
        public void A_group_of_EventSourcing_stages_when_joined_with_a_shared_event_log_can_collaborate_via_publish_subscribe()
        {
            //TODO: this is not Persistence specific
        }
    }
}