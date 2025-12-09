using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;

namespace FileMoverWeb.Controllers
{
    [ApiController]
    [Route("api/cluster")]
    public sealed class ClusterController : ControllerBase
    {
        private readonly IConfiguration _cfg;

        public ClusterController(IConfiguration cfg)
        {
            _cfg = cfg;
        }

        /// <summary>
        /// 回傳目前這台服務自己的角色資訊
        /// </summary>
        [HttpGet("self")]
        public ActionResult<SelfInfoDto> GetSelf()
        {
            var role     = _cfg["Cluster:Role"]     ?? "Slave";
            var nodeName = _cfg["Cluster:NodeName"] ?? "";
            var group    = _cfg["Cluster:Group"]    ?? "";

            var isMaster = string.Equals(role, "Master",
                System.StringComparison.OrdinalIgnoreCase);

            return new SelfInfoDto
            {
                NodeName = nodeName,
                Role     = role,
                Group    = group,
                IsMaster = isMaster
            };
        }

        public sealed class SelfInfoDto
        {
            public string NodeName { get; set; } = "";
            public string Role     { get; set; } = "";
            public string Group    { get; set; } = "";
            public bool   IsMaster { get; set; }
        }
    }
}
