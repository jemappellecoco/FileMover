using FileMoverWeb.Models;
using FileMoverWeb.Services;
using Microsoft.AspNetCore.Mvc;

namespace FileMoverWeb.Controllers;

[ApiController]
[Route("api/progress")]
public sealed class ProgressController : ControllerBase
{
    private readonly IJobProgress _progress;
    public ProgressController(IJobProgress progress) => _progress = progress;

    [HttpGet("{jobId}")]
    [Produces("application/json")]
    [ProducesResponseType(typeof(IEnumerable<TargetProgress>), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<IEnumerable<TargetProgress>> Get(string jobId)
        => Ok(_progress.Snapshot(jobId));
}
