// Models/MoveModels.cs
namespace FileMoverWeb.Models;

public sealed class MoveItem
{
    // 讓 Watcher/背景任務可帶，但對外 API 不強制
    public int? HistoryId { get; init; }        // was: required int
    public int? FileId { get; init; }           // was: required int
    public int? FromStorageId { get; init; }    // was: required int
    public int? ToStorageId { get; init; }      // was: required int

    // 既有對外 API 需要這三個，仍維持 required
    public required string SourcePath { get; init; }
    public required string DestPath   { get; init; }
    public required string DestId     { get; init; }
}

public sealed class MoveBatchRequest
{
    public required string JobId { get; init; }
    public required List<MoveItem> Items { get; init; }
    
}

public sealed class TargetProgress
{
    public required string JobId { get; set; }
    public required string DestId { get; set; }
    public long CopiedBytes { get; set; }
    public long TotalBytes  { get; set; }
    public string? CurrentFile { get; set; }     // ★ 新增
    // 0~100 夾住
    public double Percent =>
        TotalBytes <= 0 ? 0
        : Math.Min(100.0, Math.Max(0.0, (double)CopiedBytes / TotalBytes * 100.0));
}

// ====== 新增：衝突預檢 & 決策 ======

public enum ConflictKind { ExistsOnDisk, DuplicateInBatch }
public enum ConflictDecision { Overwrite, Skip, Rename }

// 預檢請求（跟 MoveBatchRequest 幾乎一樣，拆開是為了 Swagger/語意清楚）
public sealed class PrecheckRequest
{
    public required string JobId { get; init; }
    public required List<MoveItem> Items { get; init; }
}

// 預檢回應
public sealed class ConflictItem
{
    public required string SourcePath { get; init; }
    public required string DestPath   { get; init; } // 正規化後最終路徑
    public required string DestId     { get; init; }
    public required ConflictKind Kind { get; init; }

    // 顯示用參考資訊
    public long?           ExistingSize  { get; init; }
    public DateTimeOffset? ExistingMtime { get; init; }
    public long?           SourceSize    { get; init; }
    public DateTimeOffset? SourceMtime   { get; init; }
}

public sealed class PrecheckResponse
{
    public required string JobId { get; init; }
    public required List<ConflictItem> Conflicts { get; init; }
    public required List<MoveItem>     NormalizedItems { get; init; }
}

// 使用者決策之後要送進去的項目（不用繼承 MoveItem，避免 sealed 衝突）
public sealed class MoveItemDecision
{
    public required string SourcePath { get; init; }
    public required string DestPath   { get; init; }
    public required string DestId     { get; init; }

    public required ConflictDecision Decision { get; init; } // Overwrite/Skip/Rename
    public string? RenameTo { get; init; }                   // Rename 時必填（完整路徑）
}

public sealed class MoveBatchResolvedRequest
{
    public required string JobId { get; init; }
    public required List<MoveItemDecision> Items { get; init; }
}
