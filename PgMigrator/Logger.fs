namespace PgMigrator

open System
open System.IO
open Serilog
open Serilog.Core
open Serilog.Sinks.SystemConsole.Themes

type LogMode =
    private
    | ConsoleMode
    | VerboseConsoleMode
    | FileMode of logPath: string
    | VerboseFileMode of logPath: string

type LogLevel =
    | Trace = 0
    | Info = 1
    | Warn = 2
    | Fatal = 3

type MigratorLogger =
    {
      Mode: LogMode
      logTrace: string -> unit
      logInfo: string -> unit
      logWarn: string -> exn -> unit
      logError: string -> exn -> unit
    }


module Logger =
    let logToSerilog (logger: Logger) level (message: string) =
        match level with
        | LogLevel.Trace -> logger.Debug(message)
        | LogLevel.Info -> logger.Information(message)
        | LogLevel.Warn -> logger.Warning(message)
        | LogLevel.Fatal -> logger.Fatal(message)
        | _ -> logger.Error($"Unknown log level. {message}")

    let private logWithEx logger level message ex =
        let msg =
            match message with
            | m when not (String.IsNullOrWhiteSpace(m)) -> $"Message: {m}\nException: {ex}"
            | _ -> $"Exception: {ex}"

        logToSerilog logger level msg

    let getLogPath () =
        let appName = "PgMigrator"

        try
            match Environment.OSVersion.Platform with
            | PlatformID.Unix when Directory.Exists("/Library/Logs") ->
                // macOS
                let logDir =
                    Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.Personal),
                        "Library",
                        "Logs",
                        appName
                    )

                Directory.CreateDirectory(logDir) |> ignore
                Path.Combine(logDir, "error.log")
            | PlatformID.Unix ->
                // Linux
                let logDir =
                    Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                        ".local",
                        "state",
                        appName,
                        "logs"
                    )

                Directory.CreateDirectory(logDir) |> ignore
                Path.Combine(logDir, "error.log")
            | PlatformID.Win32NT ->
                // Windows
                let logDir =
                    Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                        appName,
                        "logs"
                    )

                Directory.CreateDirectory(logDir) |> ignore
                Path.Combine(logDir, "error.log")
            | _ -> failwith "Unsupported platform"
        with _ ->
            Path.Combine(Path.GetTempPath(), "error.log")

    let private createFallbackConsoleLogger isVerbose () =
        if isVerbose then
            LoggerConfiguration().MinimumLevel.Debug().WriteTo.Console(theme = SystemConsoleTheme.None).CreateLogger(), VerboseConsoleMode
        else
            LoggerConfiguration().MinimumLevel.Warning().WriteTo.Console(theme = SystemConsoleTheme.None).CreateLogger(), ConsoleMode

    let private createSerilogWithFallback isVerbose =
        try
            let logPath = getLogPath ()
            let confBuilder = LoggerConfiguration().WriteTo.File(logPath)

            if isVerbose then
                confBuilder.WriteTo.Console(theme = SystemConsoleTheme.None).CreateLogger(), VerboseFileMode logPath
            else
                confBuilder.CreateLogger(), FileMode logPath
        with
        | :? IOException as ex ->
            // Если запись в файл не удалась, используем консоль
            Console.Error.WriteLine($"Failed to write to file: {ex.Message}. Falling back to console.")
            createFallbackConsoleLogger isVerbose ()
        | ex ->
            Console.Error.WriteLine($"Unexpected error: {ex.Message}. Falling back to console.")
            createFallbackConsoleLogger isVerbose ()

    /// Функция, создающая конкретный экземпляр логгера,
    /// который умеет фильтровать сообщения по минимуму уровня логирования.
    let createLogger isVerbose =
        let logger, mode = createSerilogWithFallback isVerbose

        // Вспомогательные функции
        let logWithFilterEx logger (level: LogLevel) (message: string) (ex: exn) =
            logWithEx logger level message ex

        let logWithFilter logger (level: LogLevel) (message: string) =
            logToSerilog logger level message
        
        {
          Mode = mode
          logTrace = logWithFilter logger LogLevel.Trace
          logInfo = logWithFilter logger LogLevel.Info
          logWarn = logWithFilterEx logger LogLevel.Warn
          logError = logWithFilterEx logger LogLevel.Fatal
        }

module GlobalLogger =
    let mutable instance = Logger.createLogger false

    let setupLogger isVerbose =
        instance <- Logger.createLogger isVerbose
