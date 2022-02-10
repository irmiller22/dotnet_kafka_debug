using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;

class Consumer {

    static void Main(string[] args)
    {
        if (args.Length != 1) {
            Console.WriteLine("Please provide the configuration file path as a command line argument");
        }

        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile(args[0])
            .Build();

        configuration["group.id"] = "int-outreach-connection-debug";
        configuration["auto.offset.reset"] = "earliest";

        const string topic = "published-email-v3";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, string>(
            configuration.AsEnumerable()).Build())
        {

            consumer.Subscribe(topic);
            try {
                while (true) {
                    var cr = consumer.Consume(cts.Token);
                    Console.WriteLine($"Consumed event from topic {topic} with key {cr.Message.Key,-10} and value {cr.Message.Value}");
                    if (cr.IsPartitionEOF) {
                        Console.WriteLine($"Kafka reached end of topic {cr.Topic}, partition {cr.Partition}, offset {cr.Offset}.");
                        continue;
                    }
                }
            }
            catch (ConsumeException e) {
                Console.WriteLine("Unable to consume from topic: {e.Error.Reason}");
            }
            catch (OperationCanceledException) {
                // Ctrl-C was pressed.
            }
            finally {
                consumer.Close();
            }
        }
    }
}
