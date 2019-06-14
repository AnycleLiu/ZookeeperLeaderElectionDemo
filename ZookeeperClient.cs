using org.apache.zookeeper;
using System;
using System.Threading.Tasks;

namespace ZookeeperDemo
{
    public class ZookeeperClient
    {
        public int CONNECTION_TIMEOUT { get; set; } = 4000;

        public ZookeeperClient() { }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout">连接超时时间</param>
        public ZookeeperClient(int timeout)
        {
            CONNECTION_TIMEOUT = timeout;
        }

        public Task<ZooKeeper> CreateClient(string hostPort, string chroot = null)
        {
            var watcher = new ConnectedWatcher();

            var zk = new ZooKeeper(hostPort + chroot, CONNECTION_TIMEOUT, watcher);

            //var tcs = new TaskCompletionSource<int>();

            //watcher.SyncConnectedEvent += () =>
            //{
            //    tcs.SetResult(1);
            //};

            //if (tcs.Task != await Task.WhenAny(tcs.Task, Task.Delay(CONNECTION_TIMEOUT)))
            //{
            //    throw new Exception("Unable to connect to zookeeper server");
            //}

            return Task.FromResult(zk);
        }

        private class ConnectedWatcher : Watcher
        {
            public event Action SyncConnectedEvent;

            public override Task process(WatchedEvent @event)
            {
                if (@event.getState() == Event.KeeperState.SyncConnected)
                {
                    SyncConnectedEvent?.Invoke();
                }

                return Task.CompletedTask;
            }
        }
    }
}
