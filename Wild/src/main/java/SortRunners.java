import java.util.Scanner;

/**
 * @author Boaz Bar
 */

public class SortRunners {

    public static void main(String[] args){

        final String USER_MESSAGE = "Please enter the runner's running time";
        final String INDEX = "INDEX ";
        final String RUNNING_TIME = " running time: ";


        final int BEGIN_INDEX = 3147;
        final int NUM_RUNNERS = 10;

        Scanner scanner = new Scanner(System.in);

        double first, second, third, tmp;
        int firstIndex, secondIndex, thirdIndex;
        int tmpIndex = 0;

        System.out.println(USER_MESSAGE);
        first = scanner.nextDouble();
        firstIndex = BEGIN_INDEX;
//        System.out.println(INDEX + firstIndex + RUNNING_TIME + first);

        System.out.println(USER_MESSAGE);
        second = scanner.nextDouble();
        secondIndex = 1 + BEGIN_INDEX;
//        System.out.println(INDEX + secondIndex + RUNNING_TIME + second);

        System.out.println(USER_MESSAGE);
        third = scanner.nextDouble();
        thirdIndex = 2 + BEGIN_INDEX;
//        System.out.println(INDEX + thirdIndex + RUNNING_TIME + third);

        // sort first three inputs

        if(second > third){
            tmp = second;
            second = third;
            third = tmp;

            tmp = secondIndex;
            secondIndex = thirdIndex;
            thirdIndex = tmpIndex;
        }

        if(first > second){
            tmp = first;
            first = second;
            second = tmp;

            tmp = firstIndex;
            firstIndex = secondIndex;
            secondIndex = tmpIndex;
        }

        if(second > third){
            tmp = second;
            second = third;
            third = tmp;

            tmp = secondIndex;
            secondIndex = thirdIndex;
            thirdIndex = tmpIndex;
        }

        int counter = BEGIN_INDEX + 3;
        double input;
        // loop through the rest
        while (counter < BEGIN_INDEX + NUM_RUNNERS){

            System.out.println(USER_MESSAGE);
            input = scanner.nextDouble();
//            System.out.println(INDEX + counter + RUNNING_TIME + input);

            // case faster than first place
            if (input < first) {

                third = second;
                second = first;
                first = input;
                firstIndex = counter;
            }
            // case slower or equal to first place and faster than second
            else if(input >= first && input < second) {

                    third = second;
                    second = input;
                    secondIndex = counter;
            }
            // case slower or equal to second place and faster than third
            else if(input < third && input >= second ) {
                    third = input;
                    thirdIndex = counter;
            }

            counter++;
        }

        scanner.close();

        System.out.println();
        System.out.println();
        System.out.println("First Place: " + first + "  index " + firstIndex);
        System.out.println("Second Place: " + second + "  index " + secondIndex + " slower than first place by " + (second - first));
        System.out.println("Third Place: " + third + "  index " + thirdIndex + " slower than first place by " + (third - first));
    }


}
