using ConsoleAppFramework;
using VKV.Cli;

var app = ConsoleApp.Create();
app.Add<Commands>();
app.Run(args);