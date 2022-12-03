using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.PowerBI.Api;
using Microsoft.PowerBI.Api.Models;
using System.Text.Encodings.Web;
using System.Text.Json;

var configBuilder = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);


IConfigurationRoot configuration = configBuilder.Build();
// Add services to the container.

var connectionStringOverride = configuration.GetValue<string>("ConnectionStringOverride");

var workspaces = new List<Workspace>();

configuration.Bind("Workspaces", workspaces);

foreach (var workspace in workspaces)
{
    workspace.Initialize();
}

var workspaceLookup = workspaces.ToDictionary(w => w.Id);


var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

app.Urls.Add("https://localhost:3000");
// Configure the HTTP request pipeline.

app.UseHttpsRedirection();

var serOpts = new JsonSerializerOptions();
serOpts.WriteIndented = true;
serOpts.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;

string sessionId = null;
AdomdConnection con = null;

app.MapGet("/HealthProbe", () => "yep");

app.MapPost("/v1.0/myorg/{workspaceId:Guid}/datasets/{datasetId:Guid}/executeQueries", DatasetExecuteQueries);

app.Run();


async Task<IResult> DatasetExecuteQueries(Guid workspaceId, Guid datasetId, IConfiguration config, HttpContext context, CancellationToken cancel)
{


    var ms = new MemoryStream();
    await context.Request.BodyReader.CopyToAsync(ms);
    ms.Position = 0;
    var requestString = System.Text.Encoding.UTF8.GetString(ms.ToArray());


    var request = JsonSerializer.Deserialize<DatasetExecuteQueriesRequest>(requestString, serOpts);
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
            return Results.BadRequest($"Workspace {workspaceId} not found in the server-side configuration");
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
                workspace.LoadDatasetsFromService(workspace.XmlaEndpoint, accessToken);
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



    var gzip = false;
    if (context.Request.Headers.AcceptEncoding.Any(e => e.Equals("gzip", StringComparison.OrdinalIgnoreCase)))
    {
        gzip = true;
    }


    if (con == null)
    {
        con = new AdomdConnection(constr);
        con.Open();
    }
    
    var cmd = con.CreateCommand();
    cmd.CommandText = query;
    var reader = cmd.ExecuteReader();

    var result = new DataResult(reader, con, gzip, false, app.Logger);
    return result;


    
}



public class Dataset
{
    public Guid Id { get; set; }
    public string Name { get; set; }
}

public class Workspace
{
    public Guid Id { get; set; }
    public string Name { get; set; }

    public string XmlaEndpoint { get; set; }

    public List<Dataset> Datasets { get; set; } = new List<Dataset>();

    public Dictionary<Guid, Dataset> DatasetsFromConfig { get; set; } = new Dictionary<Guid, Dataset>();
    public Dictionary<Guid, Dataset> DatasetsFromService { get; set; } = new Dictionary<Guid, Dataset>();

    public bool DatasetsLoadedFromService { get; set; } = false;
    public void Initialize()
    {

        
        if (String.IsNullOrEmpty(XmlaEndpoint))
        {
            var nameEncoded = UrlEncoder.Default.Encode(Name);
            XmlaEndpoint = $"powerbi://api.powerbi.com/v1.0/myorg/{nameEncoded}";

        }
        DatasetsFromConfig = Datasets.ToDictionary(d => d.Id);

    }

    internal void LoadDatasetsFromService(string dataSource, string accessToken)
    {
        var constr = $"Data Source={dataSource};User Id=;Password={accessToken};";
        using (var con = new AdomdConnection(constr))
        {
            con.Open();
            var cmd = con.CreateCommand();
            cmd.CommandText = "select * from $System.DBSCHEMA_CATALOGS";
            using (var rdr = cmd.ExecuteReader())
            {
                int catalogNamePos = rdr.GetOrdinal("CATALOG_NAME");
                int databaseIdPos = -1;
                for (int i = 0; i < rdr.FieldCount; i++)
                {
                    if (rdr.GetName(i) == "DATABASE_ID")
                    {
                        databaseIdPos = i;
                        break;
                    }
                }
                while (rdr.Read()) 
                {
                    var name = rdr.GetString(catalogNamePos);
                    var id = (databaseIdPos>-1) ? rdr.GetGuid(databaseIdPos) : Guid.Empty;

                    this.DatasetsFromService.Add(id, new Dataset() { Id = id, Name = name });
                }
            }


        }
    }
}
public class CorkspaceConfiguration
{
    public Dictionary<Guid, Workspace> Workspaces { get; set; } = new Dictionary<Guid, Workspace>();


    
    
}
