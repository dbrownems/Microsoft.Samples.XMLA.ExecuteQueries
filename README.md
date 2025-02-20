# Microsoft.Samples.XMLA.ExecuteQueries

This sample provides an  API endpoint for executing DAX queries in Power BI.  It's a custom .NET Web API, but is API-compatible to the built-in ExecuteQueriesInGroup REST API.

https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group

Since it's custom code, you can avoid certian limitations of the built-in API.  Particuallry the built-in API has throttling limits, and doesn't support impersonation when calling with a Service Principal.

https://powerbi.microsoft.com/en-us/blog/executequeries-rest-api-versus-xmla-endpoints-at-scale/

These limitations don't apply to this custom endpoint since it connects directly to the Workspace XMLA endpoint.
