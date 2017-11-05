using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Linq;
using System.Threading.Tasks;
using FFCG.MemoryLeak.Writer.Contract;
using FFCG.MemoryLeak.Writer.Domain;
using Microsoft.AspNetCore.Mvc;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting.Client;

namespace FFCG.MemoryLeak.WebApi.Controllers
{
    [Route("api/[controller]")]
    public class ValuesController : Controller
    {
        private Random _random;

        public ValuesController()
        {
            this._random = new Random();
        }

        // GET api/values
        [HttpGet]
        public async Task<long[]> Get()
        {
            List<Task<long>> queueCounts = new List<Task<long>>();
            var uri = new Uri("fabric:/FFCG.MemoryLeak.Application/DocumentWriterService");
            using (var client = new FabricClient())
            {
                var partitions = await client.QueryManager.GetPartitionListAsync(uri);

                foreach (var partition in partitions)
                {
                    Debug.Assert(partition.PartitionInformation.Kind == ServicePartitionKind.Int64Range);
                    var partitionInformation = (Int64RangePartitionInformation)partition.PartitionInformation;
                    var proxy = ServiceProxy.Create<IDocumentWriterService>(uri, new ServicePartitionKey(partitionInformation.LowKey));
                    queueCounts.Add(proxy.QueueSize());
                }
            }

            return await Task.WhenAll(queueCounts);
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public string Get(int id)
        {
            return "value";
        }

        // POST api/values
        [HttpPost]
        public async Task Post()
        {
            var uri = new Uri("fabric:/FFCG.MemoryLeak.Application/DocumentWriterService");
            var client = ServiceProxy.Create<IDocumentWriterService>(uri, CreatePartitionKey());
            var payload = new byte[512];

            _random.NextBytes(payload);

            var document = new DocumentState
            {
                Payload = payload
            };
            
            await client.AddToQueue(document);
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }

        private ServicePartitionKey CreatePartitionKey()
        {
            var bytes = new byte[8];
            _random.NextBytes(bytes);

            var partitionKey = BitConverter.ToInt64(bytes, 0);
            return new ServicePartitionKey(partitionKey);
        }
    }
}
