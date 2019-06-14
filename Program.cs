using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using org.apache.zookeeper.recipes.leader;
using System;
using System.Threading.Tasks;

namespace ZookeeperDemo
{
    class Program
    {
        //static async Task Main(string[] args)
        //{
        //    var services = new ServiceCollection()
        //        .AddLogging(builder => builder.AddConsole());

        //    var serviceProvider = services.BuildServiceProvider();

        //    var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();

        //    var leaderClient = new ZookeeperLeaderClient("192.168.70.131:2181", loggerFactory);

        //    var logger = loggerFactory.CreateLogger<Program>();

        //    leaderClient.ElectedLeaderEvent += () =>
        //    {
        //        logger.LogInformation("成功竞选成为leader");
        //        return Task.CompletedTask;
        //    };

        //    await leaderClient.ElectionLeader();

        //    Console.WriteLine("press any key to exists.");
        //    Console.Read();

        //    await leaderClient.Close();
        //}

        static async Task Main(string[] args)
        {
            string nodeIdentity = Guid.NewGuid().ToString();

            Console.WriteLine($"当前节点：{nodeIdentity}");

            var client = new ZookeeperClient();
            var zk = await client.CreateClient("192.168.70.131:2181");
            var electionSupport = new LeaderElectionSupport(zk, "/fairprice/leader", nodeIdentity);
            int sleepDuration = 3000;

            await electionSupport.start();
            await Task.Delay(sleepDuration);

            var leader = await electionSupport.getLeaderHostName();
            LeaderElectionAwareImp watcher = null;

            if (leader == nodeIdentity)
            {
                Console.WriteLine("当前节点成为leader");
            }
            else
            {
                watcher = new LeaderElectionAwareImp();
                watcher.ElectionComplted += () =>
                {
                    Console.WriteLine("ELECTED_COMPLETE，当前节点成为leader");
                    return Task.CompletedTask;
                };

                electionSupport.addListener(watcher);
            }

            //Console.WriteLine("press any key to exists.");
            Console.Read();

            if (watcher != null) electionSupport.removeListener(watcher);
            await electionSupport.stop();

        }

        class LeaderElectionAwareImp : LeaderElectionAware
        {
            public event Func<Task> ElectionComplted;

            public Task onElectionEvent(ElectionEventType eventType)
            {
                if (eventType == ElectionEventType.ELECTED_COMPLETE)
                {
                    if (ElectionComplted != null)
                        return ElectionComplted();
                }
                return Task.CompletedTask;
            }
        }
    }
}
