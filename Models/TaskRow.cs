namespace FileMoverWeb.Models
{
    public class TaskRow
    {
        public int HistoryId { get; set; }
        public int FileId { get; set; }
        public int FromStorageId { get; set; }
        public int ToStorageId { get; set; }
        public string FileName { get; set; } = "";
        public string FromPath { get; set; } = "";
        public string ToPath { get; set; } = "";
    }
}
