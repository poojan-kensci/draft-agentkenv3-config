using KenSci.Data.Common.Singletons;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace KenSci.Data.Common.Tests
{
    [TestClass]
    public class ConnectionsCacheTests
    {
        private readonly ConnectionsCache _connectionsCache;

        public ConnectionsCacheTests()
        {
            _connectionsCache = ConnectionsCache.Instance;
            _connectionsCache.Add("TestKey1", "TestValue1");
            _connectionsCache.Add("TestKey2", "TestValue2");
        }

        [TestMethod]
        public void ShouldContainInitialValues()
        {
            Assert.AreEqual(2, _connectionsCache.GetCount());
            Assert.AreEqual("TestValue1", _connectionsCache.GetOrNull("TestKey1"));
            Assert.AreEqual("TestValue2", _connectionsCache.GetOrNull("TestKey2"));
        }

        [TestMethod]
        public void ShouldAddAndGetValues()
        {
            _connectionsCache.Add("MyNumber", 42);
            Assert.AreEqual(42, (int) _connectionsCache.GetOrNull("MyNumber"));
        }
    }
}
