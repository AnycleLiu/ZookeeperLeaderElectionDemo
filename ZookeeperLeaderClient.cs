using Microsoft.Extensions.Logging;
using org.apache.zookeeper;
using org.apache.zookeeper.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ZookeeperDemo
{
    public class ZookeeperLeaderClient
    {
        private readonly ILogger _logger;
        private readonly System.Threading.SemaphoreSlim _semaphoreSlim = new System.Threading.SemaphoreSlim(1, 1);

        private volatile ZooKeeper _zk;
        private LeaderWatcher _watcher;
        private readonly string _hostPort;
        private readonly int _sessionTimeout;
        private readonly byte[] _nodeIdentity;

        private readonly byte[] DEFAULT_DATA = { 0x12, 0x34 };

        private const string ROOT = "/fairprice_leader";

        private string _znode;

        /// <summary>
        /// 成功竞选成为leader
        /// </summary>
        public event Func<Task> ElectedLeaderEvent;

        public ZookeeperLeaderClient(string hostPort, ILoggerFactory loggerFactory, int sessionTimeout = 4000)
        {
            if (string.IsNullOrWhiteSpace(hostPort)) throw new ArgumentNullException(nameof(hostPort));

            _hostPort = hostPort;
            _sessionTimeout = sessionTimeout;
            _nodeIdentity = Guid.NewGuid().ToByteArray();
            _logger = loggerFactory.CreateLogger<ZookeeperLeaderClient>();
        }

        public async Task EnusreInit()
        {
            if (_zk != null) return;

            await _semaphoreSlim.WaitAsync();

            try
            {
                if (_zk == null)
                {
                    var client = new ZookeeperClient(_sessionTimeout);
                    _watcher = new LeaderWatcher();
                    _zk = await client.CreateClient(_hostPort);

                    await EnsureExists(ROOT);//确保root已存在
                    await EnsureLocalNodeExists();//确保创建当前节点

                    _watcher.NodeDeletedEvent += () =>
                    {
                        return ElectionLeader();
                    };
                }
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        /// <summary>
        /// 检查本机是否已创建节点，不存在则创建
        /// </summary>
        public async Task EnsureLocalNodeExists()
        {
            var list = (await _zk.getChildrenAsync(ROOT, _watcher)).Children;
            string path;

            foreach (var node in list)
            {
                path = ROOT + "/" + node;
                byte[] data = (await _zk.getDataAsync(path, false)).Data;

                if (data.SequenceEqual(_nodeIdentity))
                {
                    _znode = path;
                    return;
                }
            }

            path = ROOT + "/";

            _logger.LogInformation("创建zookeper节点, path: {path}, value: {value}", path, new Guid(_nodeIdentity).ToString());

            _znode = await _zk.createAsync(path, _nodeIdentity, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            _logger.LogInformation("zookeper节点创建成功：{node}", _znode);
        }

        public async Task EnsureExists(string path)
        {
            Stat stat = await _zk.existsAsync(path, false);
            if (stat == null)
            {
                await _zk.createAsync(path, DEFAULT_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }

        public async Task ElectionLeader()
        {
            _logger.LogInformation("begin leader election...");

            await EnusreInit();//确保初始化

            List<string> nodes = (await _zk.getChildrenAsync(ROOT, false)).Children;

            SortedSet<string> sortedNode = new SortedSet<string>();

            foreach (var node in nodes)
            {
                sortedNode.Add(ROOT + "/" + node);
            }

            // 取出序列号最小的消息
            string first = sortedNode.First();
            string leader = first;

            // 监控序列最小节点（非本机创建节点）
            var watcher = _znode == first ? null : _watcher;

            byte[] data = (await _zk.getDataAsync(first, watcher)).Data;

            leader = new Guid(data).ToString();

            _logger.LogInformation("leader election end, the leader is : {leader}", leader);

            if (_znode == first)
            {
                _logger.LogInformation("当前节点成为leader");

                if (ElectedLeaderEvent != null) await ElectedLeaderEvent();
            }
        }

        public class LeaderWatcher : Watcher
        {
            public event Func<Task> NodeDeletedEvent;

            public override Task process(WatchedEvent @event)
            {
                if (@event.get_Type() == Event.EventType.NodeDeleted)
                {
                    if (NodeDeletedEvent != null) return NodeDeletedEvent();
                }

                return Task.CompletedTask;
            }
        }
    }
}
