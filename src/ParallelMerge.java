import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

// check http://www2.hawaii.edu/~nodari/teaching/f16/notes/notes10.pdf for details

public class ParallelMerge {
    public static void main(String... args) throws InterruptedException {
        if (args.length != 2) {
            System.out.println("Usage: ParallelMerge [Arr1 size] [Arr2 size]");
            System.exit(1);
        }

        int aLen = Integer.parseInt(args[0]);
        int bLen = Integer.parseInt(args[1]);

        int[] A = new int[aLen];
        for(int i = 1; i < aLen; i++) {
            A[i] = (int) (Math.random() * 100);
        }
        int[] B = new int[bLen];
        for(int i = 1; i < bLen; i++) {
            B[i] = (int) (Math.random() * 100);
        }

        Arrays.sort(A);
        Arrays.sort(B);

        long start = System.nanoTime();
        int[] parRes = parMerge(A, B);
        long end = System.nanoTime();
        System.out.println("parMerge completed in " + (end - start) + " nanos");

        int[] seqRes = new int[A.length + B.length];
        start = System.nanoTime();
        merge(A, B, seqRes, 0, A.length, 0, B.length, 0);
        end = System.nanoTime();
        System.out.println("seqMerge completed in " + (end - start) + " nanos");
    }

    // Given sorted @arr returns where @val should be placed in @arr to keep it sorted, it's a sort of binary search
    // rank is s.t. arr[i] < value for any i < rank
    public static int rank(int[] arr, int val, int from, int to) {
        if(to - from == 1) {
            if(val < arr[from])
                return from;
            else if(to == arr.length || val < arr[to])
                return to;
            else
                return to+1;
        }
        int mid = (from + to) / 2;

        if(arr[mid] == val)
            return mid;
        else if(arr[mid] < val)
            return rank(arr, val, mid, to);
        else
            return rank(arr, val, from, mid);
    }

    public static int[] parMerge(int[] A, int[] B) throws InterruptedException {
        // todo try to optimize with cached thread pool
        int[] res = new int[A.length + B.length];
        int step = (int) (Math.log(B.length) / Math.log(2));

        int[] b_pivots = new int[B.length / step];
        int[] a_pivots = new int[b_pivots.length];

        // parallel pivots search (rank takes O(log(n))
        final CountDownLatch platch = new CountDownLatch(b_pivots.length);
        for(int i=1; i*step-1 < B.length; i++) {
            // let b_i = b_pivots[i], a_i = a_pivots[i]
            // then: B[b_i] > A[j] for each j <= a_i
            b_pivots[i-1] = i*step - 1;

            int finalI = i;
            Thread pivSearch = new Thread(() -> {
                a_pivots[finalI -1] = rank(A, B[finalI *step-1], 0, A.length);
                platch.countDown();
            });
            pivSearch.start();
        }
        platch.await();

        // parallel merge with calculated pivots
        final CountDownLatch latch = new CountDownLatch(b_pivots.length);
        for(int i = 0; i < b_pivots.length; i++) {
            int finalI = i;
            if(i==0) {
                // Edge case: merge 1st slices of A and B
                Thread merge = new Thread(() -> {
                    merge(A, B, res, 0, a_pivots[finalI] , 0, b_pivots[finalI],0);
                    latch.countDown();
                });
                merge.start();
            } else {
                Thread merge = new Thread(() -> {
                    merge(A, B, res, a_pivots[finalI-1], a_pivots[finalI] , b_pivots[finalI-1], b_pivots[finalI],b_pivots[finalI-1] + a_pivots[finalI-1]);
                    latch.countDown();
                });
                merge.start();
            }
        }
        // Edge case: merge last slices of A and B
        Thread merge = new Thread(() -> {
            merge(A, B, res, a_pivots[a_pivots.length-1], A.length , b_pivots[b_pivots.length-1], B.length,b_pivots[b_pivots.length-1] + a_pivots[a_pivots.length-1]);
            latch.countDown();
        });
        merge.start();

        latch.await();

        return res;
    }

    public static void merge(int[] A, int[] B, int[] C, int a_from, int a_to, int b_from, int b_to, int c_from) {

        while (a_from < a_to && b_from < b_to) {
            if(A[a_from] < B[b_from])
                C[c_from++] = A[a_from++];
            else
                C[c_from++] = B[b_from++];
        }
        while (a_from < a_to)
            C[c_from++] = A[a_from++];

        while (b_from < b_to)
            C[c_from++] = B[b_from++];
    }

    public static void printArr(int[] arr) {
        for(int v : arr)
            System.out.print(v + " ");
    }

}
