package org.kafkaApp.Synopses.DFT;

import org.apache.commons.math3.complex.Complex;
import org.apache.kafka.common.serialization.Serde;
import org.kafkaApp.Serdes.SynopsesSerdes.DFTSynopsis.DFTSynopsisSerde;
import org.kafkaApp.Synopses.Synopsis;

public class DFTSynopsis extends Synopsis {
    public windowDFT ts;
    public DFTSynopsis(){
    }
    public DFTSynopsis(String[] synopsisElements) {
        super(Integer.parseInt(synopsisElements[0]), synopsisElements[1], synopsisElements[2]);

        String [] splitParams = synopsisElements[2].split(",");

        int intervalSec = Integer.parseInt(splitParams[0]);
        ts = new windowDFT(Integer.parseInt(splitParams[1]) / intervalSec, Integer.parseInt(splitParams[2]) / intervalSec, Integer.parseInt(splitParams[3]), 1, this.getSynopsisDetails());
    }


    @Override
    public Serde<? extends Synopsis> serde() {
        return new DFTSynopsisSerde();
    }

    @Override
    public void add(Object k) {
        ts.pushToValues((Double)k);
    }

    @Override
    public Object estimate(Object k) {
        return COEFtoString();
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }


    public String COEFtoString() {
        int COEFFICIENTS_TO_USE = 2;
        Complex[] fourierCoefficients = ts.getNormalizedFourierCoefficients();
        String answer = " ";
        for (int m = 1; m < COEFFICIENTS_TO_USE; m++) {
            answer = answer + fourierCoefficients[m].getReal() + "  ";

            answer = answer + fourierCoefficients[m].getImaginary() + "  ";
            if (ts.getM() > 1.4)
                answer = answer + " ERROR// " + ts.getM();
            else
                answer = answer + " // " + ts.getM();
        }

        return answer;
    }

    public static void main(String[] args) {
        // Constructing a DFTSynopsis object
        // Assuming the parameters are: UID, keyIndex, valueIndex, timestampIndex, intervalSec, basicWindowSize, slidingWindowSize, coefficientsToUse


        String[] synopsisElements = new String[]{
                "4",             // synopsisID
                "EURTRY,Forex,4,price",      // synopsisType
                "2,250,500,4" //
        };
        DFTSynopsis dftSynopsis = new DFTSynopsis(synopsisElements);
        // Adding some sample data to the DFTSynopsis
        for (int i = 0; i < 1000; i++) {
            double value = Math.sin(2 * Math.PI * i / 100);  // Sample sine wave data
            dftSynopsis.add(value);
            if(i==500){
                System.out.println("1Estimated Fourier Coefficients:");
                System.out.println(dftSynopsis.estimate(null));
            }

        }

        // Estimating the Fourier coefficients
        String coef = (String) dftSynopsis.estimate(null);  // Passing null since the estimate method doesn't use the parameter

        // Printing the estimated coefficients
        System.out.println("Estimated Fourier Coefficients:");
        System.out.println(coef);
    }

}