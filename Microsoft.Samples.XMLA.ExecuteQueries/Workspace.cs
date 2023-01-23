using Microsoft.AnalysisServices.AdomdClient;
using System.Text.Encodings.Web;

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
        try
        {
            var constr = $"Data Source={dataSource};User Id=;Password={accessToken};";
            using (var con = new AdomdConnection(constr))
            {
                con.Open();
                var cmd = con.CreateCommand();
                cmd.CommandTimeout = 90;
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
                        var id = (databaseIdPos>-1) ? Guid.Parse( rdr.GetString(databaseIdPos)) : Guid.Empty;

                        this.DatasetsFromService.Add(id, new Dataset() { Id = id, Name = name });
                    }
                }


            }
        }
        catch (Exception ex)
        {
            throw;
        }

    }
}
