using System;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CTraderOpenApiDemo; // for AppCredentials.cs

class Program
{
    static async Task Main()
    {
        string clientId = AppCredentials.ClientId;
        string clientSecret = AppCredentials.ClientSecret;
        string redirectUri = AppCredentials.RedirectUri;

        // Step 1: Build the authorization URL
        string authUrl = $"https://openapi.ctrader.com/apps/auth?response_type=code" +
                         $"&client_id={clientId}" +
                         $"&redirect_uri={Uri.EscapeDataString(redirectUri)}" +
                         $"&scope=trading";

        Console.WriteLine("Open this URL in your browser:");
        Console.WriteLine(authUrl);

        // Step 2: Start listener to catch redirect
        var listener = new HttpListener();
        listener.Prefixes.Add(redirectUri + "/");
        listener.Start();

        Console.WriteLine("Waiting for cTrader login redirect...");

        var context = listener.GetContext();
        string? code = context.Request.QueryString["code"];
        if (string.IsNullOrEmpty(code))
        {
            Console.WriteLine("No authorization code received.");
            return;
        }

        // Respond to browser
        string responseString = "<html><body>Login successful! You can close this window.</body></html>";
        byte[] httpBuffer = Encoding.UTF8.GetBytes(responseString);
        context.Response.ContentLength64 = httpBuffer.Length;
        await context.Response.OutputStream.WriteAsync(httpBuffer, 0, httpBuffer.Length);
        context.Response.OutputStream.Close();
        listener.Stop();

        Console.WriteLine("Authorization code: " + code);

        // Step 3: Exchange code for token
        string accessToken = "";
        using (var http = new HttpClient())
        {
            var data = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string,string>("grant_type", "authorization_code"),
                new KeyValuePair<string,string>("code", code!), 
                new KeyValuePair<string,string>("client_id", clientId),
                new KeyValuePair<string,string>("client_secret", clientSecret),
                new KeyValuePair<string,string>("redirect_uri", redirectUri)
            });

            var response = await http.PostAsync("https://openapi.ctrader.com/apps/token", data);
            string json = await response.Content.ReadAsStringAsync();

            Console.WriteLine("Token response:");
            Console.WriteLine(json);

            int idx = json.IndexOf("\"access_token\":\"");
            if (idx >= 0)
            {
                int start = idx + 16;
                int end = json.IndexOf("\"", start);
                accessToken = json.Substring(start, end - start);
            }
        }

        // Step 4: Connect to WebSocket with token
        if (!string.IsNullOrEmpty(accessToken))
        {
            var ws = new ClientWebSocket();

            // Use demo or live depending on your account type
            var endpoint = "wss://demo.ctraderapi.com:5036"; // JSON endpoint for demo
            // var endpoint = "wss://live.ctraderapi.com:5036"; // JSON endpoint for live

            await ws.ConnectAsync(new Uri(endpoint), CancellationToken.None);

            // Authorize
            var authorizeObj = new
            {
                payloadType = 2100, // AuthorizeReq
                payload = new
                {
                    clientId = clientId,
                    clientSecret = clientSecret,
                    accessToken = accessToken
                }
            };
            await SendAsync(ws, authorizeObj);

            // Request accounts
            var getAccountsObj = new { payloadType = 2101, payload = new { } };
            await SendAsync(ws, getAccountsObj);

            // Listen for responses
            var wsBuffer = new byte[8192];
            while (true)
            {
                var result = await ws.ReceiveAsync(new ArraySegment<byte>(wsBuffer), CancellationToken.None);
                string msg = Encoding.UTF8.GetString(wsBuffer, 0, result.Count);
                Console.WriteLine("Received: " + msg);

                try
                {
                    using var doc = JsonDocument.Parse(msg);
                    var root = doc.RootElement;

                    if (root.TryGetProperty("payloadType", out var pt))
                    {
                        int payloadType = pt.GetInt32();

                        // GetAccountsRes
                        if (payloadType == 2102 && root.TryGetProperty("payload", out var payload))
                        {
                            var accounts = payload.GetProperty("accounts");
                            if (accounts.GetArrayLength() > 0)
                            {
                                int accountId = accounts[0].GetProperty("accountId").GetInt32();
                                Console.WriteLine($"First accountId: {accountId}");

                                // Send GetAccountInfoReq
                                var getAccountInfoObj = new
                                {
                                    payloadType = 2103,
                                    payload = new { accountId }
                                };
                                await SendAsync(ws, getAccountInfoObj);

                                // Send GetOpenPositionsReq
                                var getPositionsObj = new
                                {
                                    payloadType = 2107, // GetOpenPositionsReq
                                    payload = new { accountId }
                                };
                                await SendAsync(ws, getPositionsObj);
                            }
                        }

                        // GetAccountInfoRes
                        if (payloadType == 2104 && root.TryGetProperty("payload", out var infoPayload))
                        {
                            Console.WriteLine("=== Account Info ===");
                            foreach (var prop in infoPayload.EnumerateObject())
                            {
                                Console.WriteLine($"{prop.Name}: {prop.Value}");
                            }
                        }

                        // GetOpenPositionsRes
                        if (payloadType == 2108 && root.TryGetProperty("payload", out var posPayload))
                        {
                            Console.WriteLine("=== Open Positions ===");
                            var positions = posPayload.GetProperty("positions");
                            foreach (var pos in positions.EnumerateArray())
                            {
                                Console.WriteLine($"PositionId: {pos.GetProperty("positionId")}, Symbol: {pos.GetProperty("symbolName")}, Volume: {pos.GetProperty("volume")}, PnL: {pos.GetProperty("profit")}");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Parse error: " + ex.Message);
                }
            }
        }
    }

    private static async Task SendAsync(ClientWebSocket ws, object obj)
    {
        string json = JsonSerializer.Serialize(obj);
        await ws.SendAsync(Encoding.UTF8.GetBytes(json),
                           WebSocketMessageType.Text, true, CancellationToken.None);
    }
}
