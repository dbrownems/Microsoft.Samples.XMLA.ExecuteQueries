using Microsoft.Identity.Client;
using Microsoft.PowerBI.Api;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Models = Microsoft.PowerBI.Api.Models;

internal static class Utils
{

    internal static async Task<string> GetDatasetName(string groupId, string datasetId, PowerBIClient pbiClient)
    {
        var ds = await pbiClient.Datasets.GetDatasetInGroupAsync(Guid.Parse(groupId), datasetId);

        var name = ds.Name;

        return name;
    }

    internal static async Task<long> CopyToBitBucket(this Stream s)
    {
        long br = 0;
        var buf = new byte[1024];
        while (true)
        {
            long b =  await s.ReadAsync(buf, 0, buf.Length);
            if (b>0)
            {
                br += b;
            }
            else
            {
                break;
            }
            
        }
        return br;
    }

    internal static async Task<string> GetDatasetId(string groupId, string datasetName, PowerBIClient pbiClient)
    {
        var ds = await pbiClient.Datasets.GetDatasetsInGroupAsync(Guid.Parse(groupId));

        foreach (var d in ds.Value)
        {
            if (d.Name.Equals(datasetName, StringComparison.OrdinalIgnoreCase))
            {
                return d.Id;
            }
        }


        throw new InvalidOperationException("Dataset not found");
    }



    internal static string FormatJson(string json)
    {
        var ms = new MemoryStream();
        var doc = JsonDocument.Parse(json);
        using var w = new Utf8JsonWriter(ms, new JsonWriterOptions() { Indented = true, });
        doc.WriteTo(w);
        w.Flush();
        ms.Position = 0;
        var s = System.Text.Encoding.UTF8.GetString(ms.ToArray());
        return s;
    }

    private static IConfidentialClientApplication client;
    internal static async Task<string> GetBearerTokenAsync(string clientId, string clientSecret, string tenantId)
    {
        var redirectUri = new Uri("urn:ietf:wg:oauth:2.0:oob");

        //use this resourceId for Power BI Premium
        var scope = "https://analysis.windows.net/powerbi/api/.default";
        //https://analysis.windows.net/powerbi/api/.default
        //use this resourceId for Azure Analysis Services
        //var resourceId = "https://*.asazure.windows.net";

        var authority = $"https://login.microsoftonline.com/{tenantId}";
        // var clientId = "cf710c6e-dfcc-4fa8-a093-d47294e44c66";

        if (client == null)
            client = ConfidentialClientApplicationBuilder.Create(clientId)
                                                         .WithAuthority(authority)
                                                         .WithClientSecret(clientSecret)
                                                         .Build();

        var token = await client.AcquireTokenForClient(new List<string>() { scope }).ExecuteAsync();

        return token.AccessToken;
    }
}
