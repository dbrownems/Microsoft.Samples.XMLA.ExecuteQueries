using Microsoft.PowerBI.Api.Models;
using System.Collections.Concurrent;
using System.Text.Json;

namespace Microsoft.Samples.XMLA.ExecuteQueries
{
    public static class Handlers
    {
 
        public static async Task<IResult> ExecuteQueriesInGroup(Guid workspaceId,
                                          Guid datasetId,
                                          IConfiguration config,
                                          HttpContext context,
                                          CancellationToken cancel,
                                          ILoggerFactory loggerFactory,
                                          ConcurrentDictionary<string, AdomdConnectionPool> connectionPools,
                                          Dictionary<Guid, Workspace> workspaceLookup,
                                          JsonSerializerOptions? serOpts)
        {
            
            string? connectionStringOverride = config.GetValue<string>("ConnectionStringOverride");
            var log = loggerFactory.CreateLogger("ExecuteQueriesInGroup");

            //var ms = new MemoryStream();
            //await context.Request.BodyReader.CopyToAsync(ms);
            //ms.Position = 0;
            //var requestString = System.Text.Encoding.UTF8.GetString(ms.ToArray());


            var request = await JsonSerializer.DeserializeAsync<DatasetExecuteQueriesRequest>(context.Request.Body, serOpts);
            //var request = await JsonSerializer.DeserializeAsync<DatasetExecuteQueriesRequest>(context.Request.Body, cancellationToken: cancel);
            if (request == null)
            {
                return Results.BadRequest("DatasetExecuteQueriesRequest not found in requestBody. See https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries");
            }
            if (request.Queries.Count != 1)
            {
                return Results.BadRequest("Exactly one query must be specified");
            }
            var query = request.Queries[0].Query;

            string constr;

            if (!string.IsNullOrEmpty(connectionStringOverride))
            {
                constr = connectionStringOverride;
            }
            else
            {
                if (!workspaceLookup.ContainsKey(workspaceId))
                {
                    return Results.BadRequest($"Workspace [{workspaceId}] not found in the server-side configuration");
                }

                int authHeaderCount = context.Request.Headers.Authorization.Count;
                if (authHeaderCount != 1)
                {
                    return Results.Unauthorized();
                }
                var authHeader = context.Request.Headers.Authorization[0];
                if (!authHeader.StartsWith("Bearer "))
                {
                    return Results.Unauthorized();
                }
                var accessToken = authHeader.Substring("Bearer ".Length);


                var workspace = workspaceLookup[workspaceId];


                string datasetName;
                if (workspace.DatasetsFromConfig.ContainsKey(datasetId))
                {
                    datasetName = workspace.DatasetsFromConfig[datasetId].Name;
                }
                else
                {
                    if (!workspace.DatasetsLoadedFromService)
                    {
                        lock (workspace) 
                        {
                            if (!workspace.DatasetsLoadedFromService)
                            {
                                workspace.LoadDatasetsFromService(workspace.XmlaEndpoint, accessToken);
                                workspace.DatasetsLoadedFromService = true;
                            }
                        }
                        
                    }
                    if (workspace.DatasetsFromService.ContainsKey(datasetId))
                    {
                        datasetName = workspace.DatasetsFromService[datasetId].Name;
                    }
                    else
                    {
                        return Results.BadRequest($"Dataset {datasetId} not found in configuration or in DMV.");
                    }

                }


                constr = $"Data Source={workspace.XmlaEndpoint};User Id=;Password={accessToken};Catalog={datasetName};";

                if (!string.IsNullOrEmpty(request.ImpersonatedUserName))
                {
                    constr = constr + $"EffectiveUserName={request.ImpersonatedUserName}";
                }
            }

            var pool = connectionPools.GetOrAdd(constr, c => new AdomdConnectionPool(c, log));


            var gzip = false;
            if (context.Request.Headers.AcceptEncoding.Any(e => e.Equals("gzip", StringComparison.OrdinalIgnoreCase)))
            {
                gzip = true;
            }
            //gzip = true;

            var wrapper = pool.GetWrappedConnection();
            var con = wrapper.Connection;
            context.Response.RegisterForDispose(wrapper);



            var cmd = con.CreateCommand();
            cmd.CommandText = query;
            var reader = cmd.ExecuteReader();

            var result = new DataResult(reader, con, gzip, false, log);
            return result;

        }

    }
}
