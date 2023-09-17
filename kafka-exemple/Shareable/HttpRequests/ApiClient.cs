using System;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;

namespace kafka_exemple.Shareable.HttpRequests
{
    public class ApiClient<T> : IDisposable
    {
        private readonly HttpClient _httpClient;

        public ApiClient(string baseUrl)
        {
            if (string.IsNullOrWhiteSpace(baseUrl))
            {
                throw new ArgumentException("A URL base da API deve ser fornecida.", nameof(baseUrl));
            }

            _httpClient = new HttpClient
            {
                BaseAddress = new Uri(baseUrl),
                Timeout = TimeSpan.FromSeconds(30)
            };
        }

        public async Task<T> GetAsync(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new ArgumentException("O endpoint da API deve ser fornecido.", nameof(endpoint));
            }

            try
            {
                var response = await _httpClient.GetFromJsonAsync<T>(endpoint);

                if (response is not null)
                {
                    return response;
                }

                throw new HttpRequestException($"Resposta vazia da API no endpoint: {endpoint}");
            }
            catch (HttpRequestException ex)
            {
                throw new HttpRequestException($"Falha na requisição GET: {ex.Message}");
            }
        }

        public async Task<T> PostAsync(string endpoint, T data)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new ArgumentException("O endpoint da API deve ser fornecido.", nameof(endpoint));
            }

            try
            {
                var response = await _httpClient.PostAsJsonAsync(endpoint, data);
                response.EnsureSuccessStatusCode(); // Lança uma exceção em caso de erro HTTP

                return await response.Content.ReadFromJsonAsync<T>();
            }
            catch (HttpRequestException ex)
            {
                throw new HttpRequestException($"Falha na requisição POST: {ex.Message}");
            }
        }

        public async Task<T> PutAsync(string endpoint, T data)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new ArgumentException("O endpoint da API deve ser fornecido.", nameof(endpoint));
            }

            try
            {
                var response = await _httpClient.PutAsJsonAsync(endpoint, data);
                response.EnsureSuccessStatusCode(); // Lança uma exceção em caso de erro HTTP

                return await response.Content.ReadFromJsonAsync<T>();
            }
            catch (HttpRequestException ex)
            {
                throw new HttpRequestException($"Falha na requisição PUT: {ex.Message}");
            }
        }

        public async Task<T> PatchAsync(string endpoint, object data)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new ArgumentException("O endpoint da API deve ser fornecido.", nameof(endpoint));
            }

            try
            {
                var request = new HttpRequestMessage(new HttpMethod("PATCH"), endpoint);
                request.Content = JsonContent.Create(data);

                var response = await _httpClient.SendAsync(request);

                response.EnsureSuccessStatusCode(); // Lança uma exceção em caso de erro HTTP

                return await response.Content.ReadFromJsonAsync<T>();
            }
            catch (HttpRequestException ex)
            {
                throw new HttpRequestException($"Falha na requisição PATCH: {ex.Message}");
            }
        }

        public async Task DeleteAsync(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new ArgumentException("O endpoint da API deve ser fornecido.", nameof(endpoint));
            }

            try
            {
                var response = await _httpClient.DeleteAsync(endpoint);
                response.EnsureSuccessStatusCode(); // Lança uma exceção em caso de erro HTTP
            }
            catch (HttpRequestException ex)
            {
                throw new HttpRequestException($"Falha na requisição DELETE: {ex.Message}");
            }
        }

        public void Dispose()
        {
            _httpClient.Dispose();
        }
    }
}
