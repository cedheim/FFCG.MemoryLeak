using System;
using System.Runtime.Serialization;

namespace FFCG.MemoryLeak.Writer.Domain
{
    [DataContract]
    public class DocumentState
    {
        public DocumentState()
        {
            Id = Guid.NewGuid();
        }

        [DataMember]
        public Guid Id { get; set; }

        [DataMember]
        public byte[] Payload { get; set; }
    }
}