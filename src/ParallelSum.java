import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class ParallelSum {
    public static void main(String... args) throws InterruptedException {
        if (args.length != 2) {
            System.out.println("Usage: ParallelSum [array size] [n processors]");
            System.exit(1);
        }

        int arraySize = Integer.parseInt(args[0]);
        int nProcessors = Integer.parseInt(args[1]);
        if (nProcessors > arraySize / 2) {
            System.out.println("Sure");
            System.exit(1);
        }

        long[] arr = new long[arraySize];
        for(int i = 1; i < arraySize; i++) {
            arr[i] = (long) (Math.random() * 10);
        }

        System.out.println("Running with arr size " + arraySize + " and " + nProcessors + " threads");

        long start = System.nanoTime();
        long res = seqSum(arr);
        long end = System.nanoTime();
        System.out.println("seqSum completed in " + (end - start) + " nanos");
        System.out.println(res);

        start = System.nanoTime();
        res = parallelSum(arr, nProcessors);
        end = System.nanoTime();
        System.out.println("parSum completed in " + (end - start) + " nanos");
        System.out.println(res);
    }

    public static long parallelSum(long[] arr, int nProcessors) throws InterruptedException {
        AtomicLong sum = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch(nProcessors);
        final int step = arr.length / nProcessors;

        // Run one summing thread for each @nProcessors slice of arr
        for(int i = 0; i < nProcessors; i++) {
            int from = i * step;

            if(i < nProcessors -1) {
                Thread calc = new Thread(() -> {
                    sum.getAndAdd(partialSum(arr, from, from + step));
                    latch.countDown();
                });
                calc.start();
            } else {
                Thread calc = new Thread(() -> {
                    sum.getAndAdd(partialSum(arr, from, arr.length));
                    latch.countDown();
                });
                calc.start();
            }
        }
        latch.await();
        return sum.get();
    }

    public static long seqSum(long[] arr) {
        long seqSum = 0;
        for (long value : arr) seqSum += value;
        return seqSum;
    }

    public static long partialSum(long[] arr, int from, int to) {
        long s = 0;
        for(int i = from; i < to; i++)
            s += arr[i];
        return s;
    }
}