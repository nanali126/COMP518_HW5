package ecg;

import dsl.S;
import dsl.Q;
import dsl.Query;

public class PeakDetection {

	// The curve length transformation:
	//
	// adjust: x[n] = raw[n] - 1024
	// smooth: y[n] = (x[n-2] + x[n-1] + x[n] + x[n+1] + x[n+2]) / 5
	// deriv: d[n] = (y[n+1] - y[n-1]) / 2
	// length: l[n] = t(d[n-w]) + ... + t(d[n+w]), where
	//         w = 20 (samples) and t(d) = sqrt(1.0 + d * d)

	public static Query<Integer,Double> qLength() {
		Query<Integer,Integer> adjust = Q.map(x -> x - 1024);
		Query<Integer,Integer> smoothSum = Q.sWindowNaive(5, 0, Integer::sum);
		Query<Integer,Double> smoothDiv = Q.map(s -> s / 5.0);
		Query<Double,Double> deriv = Q.sWindow3((a, b, c) -> (c - a) / 2.0);
		Query<Double,Double> length = Q.sWindowNaive(41, 0.0,
			(acc, d) -> acc + Math.sqrt(1.0 + d * d));
		return Q.pipeline(adjust, smoothSum, smoothDiv, deriv, length);
	}

	// In order to detect peaks we need both the raw (or adjusted)
	// signal and the signal given by the curve length transformation.
	// Use the datatype VTL and implement the class Detect.

	public static Query<Integer,Long> qPeaks() {
		Query<Integer,VT> branch1 = Q.pipeline(
			Q.scan(new VT(0, -1), (prev, raw) -> new VT(raw - 1024, prev.ts + 1)),
			Q.ignore(23)
		);
		Query<Integer,Double> branch2 = qLength();
		Query<Integer,VTL> combined = Q.parallel(branch1, branch2,
			(vt, l) -> vt.extendl(l));
		return Q.pipeline(combined, new Detect());
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for Peak Detection *****");
		System.out.println("****************************************");
		System.out.println();

		Q.execute(Data.ecgStream("100.csv"), qPeaks(), S.printer());
	}

}
