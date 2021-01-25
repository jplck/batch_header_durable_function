using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace HeaderVerifier
{
    public class Header
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("lines")]
        public List<string> Lines { get; set; }

        public bool HasHeader => Lines.Count > 0;

        [JsonProperty("isPartial")]
        public bool IsPartial { get; set; }

        public Header()
        {
            Lines = new List<string>();
            IsPartial = true;
            Id = Guid.NewGuid().ToString();
        }
    }

    [JsonObject(MemberSerialization.OptIn)]
    class StatusEntity: IStatusEntity
    {
        [JsonProperty("header")]
        private Header header { get; set; }

        public Task<Header> GetHeaderAsync() =>  Task.FromResult(header);

        public void SetHeaderAsync(Header header) => this.header = header;

        public void Reset() => header = new Header();

        [FunctionName(nameof(StatusEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx)
        => ctx.DispatchAsync<StatusEntity>();
    }

    public interface IStatusEntity
    {
        Task<Header> GetHeaderAsync();

        void SetHeaderAsync(Header header);

        void Reset();
    }
}
