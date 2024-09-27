namespace CassandraAPI.services
{
    using Cassandra;
    public class CassandraService
    {
        private readonly ISession _session;

        public CassandraService(IConfiguration configuration)
        {
            var contactPoints = configuration.GetSection("Cassandra:ContactPoints").Get<string[]>();
            var keyspace = configuration.GetSection("Cassandra:Keyspace").Value;

            var cluster = Cluster.Builder()
                                 .AddContactPoints(contactPoints)
                                 .Build();

            _session = cluster.Connect(keyspace);
        }

        public RowSet ExecuteQuery(string query)
        {
            return _session.Execute(query);
        }
    }
}
