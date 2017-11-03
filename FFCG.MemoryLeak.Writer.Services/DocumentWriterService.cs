using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FFCG.MemoryLeak.Writer.Contract;
using FFCG.MemoryLeak.Writer.Domain;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;

namespace FFCG.MemoryLeak.Writer.Services
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class DocumentWriterService : StatefulService, IDocumentWriterService
    {
        private const string QueueName = "InputQueue";
        private readonly Random _random = new Random();
        
        public DocumentWriterService(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[] { new ServiceReplicaListener(this.CreateServiceRemotingListener) };
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            var queue = await StateManager.GetOrAddAsync<IReliableConcurrentQueue<DocumentState>>(QueueName);
            var stopwatch = new Stopwatch();

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {

                    cancellationToken.ThrowIfCancellationRequested();
                    bool managedToDequeue;
                    using (var transaction = StateManager.CreateTransaction())
                    {
                        var conditionalValue = await queue.TryDequeueAsync(transaction, cancellationToken);
                        managedToDequeue = conditionalValue.HasValue;

                        if (conditionalValue.HasValue)
                        {
                            stopwatch.Reset();
                            stopwatch.Start();

                            var state = conditionalValue.Value;
                            var tasks = new List<Task>
                            {
                                Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken)
                            };

                            await Task.WhenAll(tasks);
                            stopwatch.Stop();
                            ServiceEventSource.Current.ServiceMessage(Context,
                                $"DocumentWriterService - Dequeued {state.Id}, Time: {stopwatch.ElapsedMilliseconds}");
                        }
                        await transaction.CommitAsync();
                    }

                    if (!managedToDequeue)
                        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                }
                catch (TimeoutException e)
                {
                    ServiceEventSource.Current.ServiceMessage(Context,
                        $"DocumentWriterService: TimeoutException in RunAsync: {e.Message}");
                    await Task.Delay(TimeSpan.FromMilliseconds(_random.NextDouble() * 50.0 + 50.0),
                        cancellationToken);
                }
                catch (FabricTransientException fte)
                {
                    // Transient exceptions can be retried without faulting the replica.
                    // Instead of retrying here, simply move on to the next iteration after a delay (set below).
                    ServiceEventSource.Current.ServiceMessage(Context,
                        $"DocumentWriterService: FabricTransientException in RunAsync: {fte.Message}.");
                    await Task.Delay(TimeSpan.FromMilliseconds(_random.NextDouble() * 50.0 + 50.0),
                        cancellationToken);
                }
                catch (FabricNotPrimaryException)
                {
                    // This replica is no longer primary, so we can exit gracefully here.
                    ServiceEventSource.Current.ServiceMessage(Context,
                        $"DocumentWriterService: RunAsync is exiting because the replica is no longer primary.");
                    return;
                }
                catch (OperationCanceledException)
                {
                    // this means the service needs to shut down. Make sure it gets re-thrown.
                    throw;
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        public async Task AddToQueue(DocumentState state)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var queue = await this.StateManager.GetOrAddAsync<IReliableConcurrentQueue<DocumentState>>(QueueName);

            while (true)
            {
                try
                {
                    using (var transaction = this.StateManager.CreateTransaction())
                    {
                        await queue.EnqueueAsync(transaction, state);
                        await transaction.CommitAsync();
                    }

                    break;
                }
                catch (TimeoutException)
                {
                    ServiceEventSource.Current.ServiceMessage(Context, $"DocumentWriterService - TimeoutException in AddToQueue");

                    await Task.Delay(100, CancellationToken.None);
                }
            }
            stopwatch.Stop();

            ServiceEventSource.Current.ServiceMessage(Context, $"DocumentWriterService - Queued {state.Id}, Time: {stopwatch.ElapsedMilliseconds}");
        }
    }
}
