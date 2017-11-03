using System.Threading.Tasks;
using FFCG.MemoryLeak.Writer.Domain;
using Microsoft.ServiceFabric.Services.Remoting;

namespace FFCG.MemoryLeak.Writer.Contract
{
    public interface IDocumentWriterService : IService
    {
        Task AddToQueue(DocumentState state);
    }
}