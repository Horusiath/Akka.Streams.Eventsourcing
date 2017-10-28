using System;

namespace Akka.Streams.Eventsourcing
{
    /// <summary>
    /// Event delivery protocol implemented by event logs and sources.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class Delivery<T> { }

    /// <summary>
    /// Emitted by an event log to signal recovery completion.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class Recovered<T> : Delivery<T>
    {
        public static readonly Recovered<T> Instance = new Recovered<T>();
        private Recovered() { }
    }

    /// <summary>
    /// Emitted by an event log or source to deliver an event (replayed or alive).
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class Delivered<T> : Delivery<T>
    {
        public Delivered(T data)
        {
            Data = data;
        }

        public T Data { get; }
    }

    /// <summary>
    /// Metadata and container of an event emitted by an `EventSourcing` stage.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class Emitted<T>
    {
        public string EmitterId { get; }
        public Guid EmissionId { get; }
        public T Event { get; }

        public Emitted(T @event, string emitterId, Guid emissionId)
        {
            EmitterId = emitterId;
            EmissionId = emissionId;
            Event = @event;
        }

        public Emitted(T @event, string emitterId) : this(@event, emitterId, Guid.NewGuid())
        {
        }

        public Durable<T> ToDurable(long sequenceNr) => new Durable<T>(Event, EmitterId, sequenceNr, EmissionId);
    }

    /// <summary>
    /// Metadata and container of a durable event emitted by an event log or source.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class Durable<T>
    {
        public string EmitterId { get; }
        public long SequenceNr { get; }
        public Guid EmissionId { get; }
        public T Event { get; }

        public Durable(T @event, string emitterId, long sequenceNr, Guid emissionId)
        {
            EmitterId = emitterId;
            SequenceNr = sequenceNr;
            EmissionId = emissionId;
            Event = @event;
        }
    }
}