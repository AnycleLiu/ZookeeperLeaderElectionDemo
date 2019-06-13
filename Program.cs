using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace ZookeeperDemo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var services = new ServiceCollection()
                .AddLogging(builder => builder.AddConsole());

            var serviceProvider = services.BuildServiceProvider();

            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();

            var leaderClient = new ZookeeperLeaderClient("192.168.70.131:2181", loggerFactory);

            var logger = loggerFactory.CreateLogger<Program>();

            leaderClient.ElectedLeaderEvent += () =>
            {
                logger.LogInformation("成功竞选成为leader");
                return Task.CompletedTask;
            };

            await leaderClient.ElectionLeader();

            Console.WriteLine("press any key to exists.");
            Console.Read();
        }

    }
}
