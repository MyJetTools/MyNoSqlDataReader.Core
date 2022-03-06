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
    
    public void WriteInfoLog(string message)
    {
        if (_logCallback == null)
        {
            Console.WriteLine("MyNoSqlDataReader Info: " + message);
        }
        else
        {
            _logCallback(LogItem.Create(LogLevel.Info, "MyNoSqlTcpDataReader", message, null));
        }
        
    }
    
    public void WriteErrorLog(Exception e)
    {
        if (_logCallback == null)
        {
            Console.WriteLine("MyNoSqlDataReader Error: " + e.Message);
            Console.WriteLine(e);
        }
        else
        {
            _logCallback(LogItem.Create(LogLevel.Error, "MyNoSqlTcpDataReader", e.Message, e));
        }
    }
    
    private void PlugLogger(Action<LogItem> logCallback)
    {
        _logCallback = logCallback;
    }
}