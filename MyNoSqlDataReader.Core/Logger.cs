namespace MyNoSqlDataReader.Core;

public class Logger
{
    public enum LogLevel{
        Error, Info
    }
    
    public struct LogItem
    {
        public DateTime DateTime { get; private set; }
        public LogLevel Level { get; private set; }
        public string Process { get; private set; }
        public string Message { get; private set; }
        public Exception? Exception { get; private set; }
        
        public static LogItem Create(LogLevel logLevel, string process, string message, Exception? exception)
        {
            return new LogItem
            {
                DateTime = DateTime.UtcNow,
                Level = logLevel,
                Process = process,
                Message = message,
                Exception = exception
            };
        }
    }
    
    private Action<LogItem>? _logCallback;
    
    public void Write(LogItem item)
    {
        if (_logCallback == null)
        {
            Console.WriteLine("==== Socket Log Record =====");
            Console.WriteLine($"DateTime: {item.DateTime:s}");
            Console.WriteLine($"Level: Info: {item.Level}");
            Console.WriteLine($"Process: {item.Process}");
            Console.WriteLine($"Message: {item.Message}");
        }
        else
        {
            _logCallback(item);
        }
    }
    
    private void PlugLogger(Action<LogItem> logCallback)
    {
        _logCallback = logCallback;
    }
}