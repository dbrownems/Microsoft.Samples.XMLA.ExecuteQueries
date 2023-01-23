namespace Microsoft.Samples.XMLA.ExecuteQueries
{
    using Microsoft.AnalysisServices.AdomdClient;
    using Microsoft.Extensions.ObjectPool;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Text;

    public sealed class WrappedConnection : IDisposable
    {
        AdomdConnection con;
        AdomdConnectionPool pool;

        public AdomdConnectionPool Pool => pool;
        public AdomdConnection Connection => con;
        public WrappedConnection(AdomdConnectionPool pool, AdomdConnection con)
        {
            this.con = con;
            this.pool = pool;
       }

        public void Dispose()
        {
            pool.ReturnConnection(con);  
        }
    }

    internal class SessionStats
    {
        public SessionStats(string sessionId)
        {
            this.SessionId = sessionId;
        }
        public string SessionId { get;  }
        public DateTime StartTime { get; } = DateTime.Now;
        public int UseCount { get; set; } = 0;
        public int ReturnCount { get; set; } = 0;
    }

    public class AdomdConnectionPool : IPooledObjectPolicy<AdomdConnection>
    {

        private readonly string connectionString;
        private ILogger log;

        public string RedactedConnectionString {get;}
        volatile int getConnectionCount = 0;
        volatile int createConnectionCount = 0;
        volatile int returnConnectionCount = 0;
        volatile int nonReusableConnectionReturnedCount = 0;

        ConcurrentDictionary<string, SessionStats> sessionStats = new ConcurrentDictionary<string, SessionStats>();
        ObjectPool<AdomdConnection> pool;

        public AdomdConnectionPool(string connectionString, ILogger log, int maxRetained = 500)
        {
            this.connectionString = connectionString;
            this.log = log;
            var pp = new DefaultObjectPoolProvider();
            pp.MaximumRetained = maxRetained;
            pool = pp.Create<AdomdConnection>(this);

            var sb = new StringBuilder();
            foreach (var s in connectionString.Split(';'))
            {
                if (!s.TrimStart().StartsWith("password", StringComparison.OrdinalIgnoreCase))
                    sb.Append(s).Append(';');
                else
                    sb.Append("password=").Append(s.GetHashCode()).Append(';');
            }
            RedactedConnectionString = sb.ToString();
        }

        public int SessionCount  => sessionStats.Count;
        
        public WrappedConnection GetWrappedConnection()
        {
            var w = new WrappedConnection(this, this.GetConnection());
            return w;
        }

        public string PoolStatusString()
        {
            return $"Sessions: {SessionCount}. Gets:{getConnectionCount} Returns:{returnConnectionCount} Creates:{createConnectionCount} NonReusables:{nonReusableConnectionReturnedCount}";
        }
        void WarmUp()
        {
            int n = 4;
            log.LogInformation($"Starting warming Connection Pool with {n} connections");
            var cons = new ConcurrentBag<AdomdConnection>();

            for (int i = 0; i < n; i++)
            {
                cons.Add(pool.Get());
            }


            foreach (var c in cons)
            {
                pool.Return(c);
            }
            log.LogInformation($"Completed warming Connection Pool with {n} connections");

        }

        /// <summary>
        /// Set limits on connection lifetime.
        /// TOTO: refine time parameters with testing/PG guidance
        /// </summary>
        /// <param name="con"></param>
        /// <returns></returns>
        bool IsSessionValidForCheckIn(AdomdConnection con)
        {
            var stats = sessionStats[con.SessionID];
            var openFor = DateTime.Now.Subtract(stats.StartTime);
            var rv = (con.State == System.Data.ConnectionState.Open && openFor < TimeSpan.FromMinutes(20));
            return rv;

        }
        bool IsSessionValidForCheckOut(AdomdConnection con)
        {
            var stats = sessionStats[con.SessionID];
            var openFor = DateTime.Now.Subtract(stats.StartTime);
            var rv = (openFor < TimeSpan.FromMinutes(25));
            return rv;
        }

        /// <summary>
        /// Runs a trivial command on the connection before returning it
        /// </summary>
        /// <returns></returns>
        public AdomdConnection GetValidatedConnection()
        {
            var con = GetConnection();
            int retries = 0;
            while (true)
            {
                var cmd = con.CreateCommand();
                cmd.CommandText = "";
                try
                {
                    var sw = new Stopwatch();
                    sw.Start();
                    cmd.ExecuteNonQuery();

                    log.LogInformation($"Connection validated on checkout in {sw.ElapsedMilliseconds:F0}ms");
                    return con;
                }
                catch (AdomdConnectionException ex)
                {
                    log.LogWarning($"Connection failed validation on checkout {ex.GetType().Name} : {ex.Message}");
                    con.Dispose();
                    con = GetConnection();
                    retries++;
                }
            }
        }
        
         public AdomdConnection GetConnection()
        {
            Interlocked.Increment(ref getConnectionCount);

            AdomdConnection con;
            try
            {
                con = pool.Get();
                sessionStats[con.SessionID].UseCount++;
            }
            catch (Exception ex)
            {
                throw;
            }


            while (con.State != System.Data.ConnectionState.Open || !IsSessionValidForCheckOut(con))
            {
                log.LogInformation("Retrieved connection that either was not Open or whose session has timed out.");
                ReturnConnection(con);
                con = pool.Get();
            }

            return con;
        }

        public void ReturnConnection(AdomdConnection con)
        {

            if (con == null)
                throw new ArgumentException("AdomdConnection object is null at ReturnConnection");

            Interlocked.Increment(ref returnConnectionCount);

            if (con.State != System.Data.ConnectionState.Open)
            {
                con.Dispose();
                return;
            }

            string sessionId;
            try
            {
                sessionId = con.SessionID;
            }
            catch (NullReferenceException)
            {
                con.Dispose();
                return;
            }

            sessionStats[con.SessionID].ReturnCount++;
            pool.Return(con);
        }



        AdomdConnection IPooledObjectPolicy<AdomdConnection>.Create()
        {
            Interlocked.Increment(ref this.createConnectionCount);

            var constr = this.connectionString;
            var con = new AdomdConnection(constr);

            var sw = new Stopwatch();
            sw.Start();
            con.Open();

            var sessionId = con.SessionID;
            con.Disposed += (s, a) =>
                {
                    if (sessionStats.Remove(sessionId, out var stats))
                    {
                        var dt = stats.StartTime;

                        log.LogWarning($"AdomdConnection for session {sessionId} Disposed. Session Duration {DateTime.Now.Subtract(dt):c} Reused: {stats.UseCount} Returned: {stats.ReturnCount}");
                    }
                   
                };
            

            if (sw.ElapsedMilliseconds > 4000)
            {
                log.LogWarning($"AdomdConnection.Open succeeded in {sw.ElapsedMilliseconds}ms");
            }
            var stats = new SessionStats(con.SessionID);
            sessionStats.AddOrUpdate(stats.SessionId, s => stats, (s, d) => stats);
            log.LogInformation("Creating new pooled connection");

            return con;
        }

        bool IPooledObjectPolicy<AdomdConnection>.Return(AdomdConnection con)
        {
            
            if (con.State != System.Data.ConnectionState.Open || !IsSessionValidForCheckIn(con))
            {
                Interlocked.Increment(ref nonReusableConnectionReturnedCount);
                log.LogInformation($"Connection cannot be reused. Closing State: {con.State} {con.SessionID}");
                sessionStats.Remove(con.SessionID, out _);
                con.Dispose();
                return false;
            }



            return true;
        }

        //public class ConnectionOptions
        //{
        //    private bool UseManagedIdentity { get; set; }  //not implemented
        //    public string ClientId { get; set; }
        //    public string ClientSecret { get; set; }
        //    public string TenantId { get; set; }
        //    public string XmlaEndpoint { get; set; }
        //    public string DatasetName { get; set; }
        //    private string EffectiveUserName { get; set; }//not implemented

        //    public void Validate()
        //    {
        //        if (!UseManagedIdentity && (ClientId == null || ClientSecret == null || TenantId == null))
        //        {
        //            throw new ArgumentException("If not Using Manged Identity, ClientId, ClientSecret, and TenantId are required.");
        //        }
        //        if (XmlaEndpoint == null || !Uri.IsWellFormedUriString(XmlaEndpoint, UriKind.Absolute))
        //        {
        //            throw new ArgumentException("XmlaEndpoint Uri is required, and must be a valid Uri.");
        //        }
        //        if (DatasetName == null)
        //        {
        //            throw new ArgumentException("DatasetName is required.  Specify the name of the Dataset or Database.");
        //        }
        //    }


        //}


    }

}
