//HistoryModels.cs
namespace FileMoverWeb.Services
{
    public sealed class HistoryRow
    {
        public int HistoryId { get; set; }
        public int FileId { get; set; }
        public string? FileName { get; set; }
        public string? UserBit { get; set; }
       

        public int FromStorageId { get; set; }
        public string? FromName { get; set; }
        public string? FromPath { get; set; }

        public int ToStorageId { get; set; }
        public string? ToName { get; set; }
        public string? ToPath { get; set; }

        public string? RequestedBy { get; set; }
        public string? Action { get; set; }
        public int Status { get; set; }           // 0/1/10/90
        public DateTime CreateTime { get; set; }
        public DateTime UpdateTime { get; set; }

        public string? Error { get; set; }    
    }
}
