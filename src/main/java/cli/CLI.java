package cli;

import tributary.Tributary;
import tributary.RebalancingMethod;
import tributary.AllocationMethod;

import java.util.Arrays;
import java.util.Scanner;
import java.util.List;

public class CLI {
    public static void main(String[] args) {
        Tributary<?> tributary = new Tributary<>();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("> ");
            String[] command = scanner.nextLine().split(" ");

            try {
                switch (command[0]) {
                    case "createTopic":
                        tributary.createTopic(command[1], command[2]);
                        break;
                    case "createPartition":
                        tributary.createPartition(command[1], command[2]);
                        break;
                    case "createConsumerGroup":
                        RebalancingMethod rebalancingMethod = RebalancingMethod.valueOf(command[3].toUpperCase());
                        tributary.createConsumerGroup(command[1], command[2], rebalancingMethod);
                        break;
                    case "createConsumer":
                        tributary.createConsumer(command[1], command[2]);
                        break;
                    case "deleteConsumer":
                        tributary.deleteConsumer(command[1], command[2]);
                        break;
                    case "createProducer":
                        AllocationMethod allocationMethod = AllocationMethod.valueOf(command[3].toUpperCase());
                        tributary.createProducer(command[1], command[2], allocationMethod.name());
                        break;
                    case "produceEvent":
                        tributary.produceEvent(command[1], command[2], command[3], command[4]);
                        break;
                    case "consumeEvent":
                        tributary.consumeEvent(command[1], command[2]);
                        break;
                    case "consumeEvents":
                        int numberOfEvents = Integer.parseInt(command[3]);
                        tributary.consumeEvents(command[1], command[2], numberOfEvents);
                        break;
                    case "showTopic":
                        tributary.showTopic(command[1]);
                        break;
                    case "showConsumerGroup":
                        tributary.showConsumerGroup(command[1]);
                        break;
                    case "parallelProduce":
                        String[] eventFileNames = command[3].split(",");
                        List<String> eventFileNamesList = Arrays.asList(eventFileNames);
                        tributary.parallelProduce(command[1], command[2], eventFileNamesList);
                    case "parallelConsume":
                        int numberEvents = Integer.parseInt(command[3]);
                        tributary.parallelConsume(command[1], command[2], numberEvents);
                        break;
                    case "setConsumerGroupRebalancing":
                        RebalancingMethod rebalance = RebalancingMethod.valueOf(command[2].toUpperCase());
                        tributary.setConsumerGroupRebalancing(command[1], rebalance);
                        break;
                    case "playback":
                        int offset = Integer.parseInt(command[3]);
                        tributary.playback(command[1], command[2], offset);
                        break;
                    case "exit":
                        System.out.println("Exiting Tributary...");
                        scanner.close();
                        return;
                    default:
                        System.out.println("command is Invalid");
                        break;
                }
            } catch (Exception exception) {
                System.out.println("invalid command or parameters");
            }
        }
    }
}

