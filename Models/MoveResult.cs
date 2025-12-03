namespace FileMoverWeb.Models
{
    public class MoveResult
    {
        public int HistoryId { get; set; }     // FileData_History.id
        public int FileId { get; set; }        // FileData.id (可選)
        public int FromStorageId { get; set; } // (可選)
        public int ToStorageId { get; set; }   // (可選)
        public bool Success { get; set; }
        public string? Error { get; set; }
        
        // ★ 新增：要寫回 DB 的 file_status（例如 911 / 912 / 91）
        public int? StatusCode { get; set; } 
    }
}
