package org.kafkaApp.Synopses.DFT;

import org.apache.commons.math3.complex.Complex;

import java.io.Serializable;
import java.util.LinkedList;



public class windowDFT implements Serializable {

    private  int basicWindowSize ; // The size of the basic window
    private  int slidingWindowSize;
    private  int NofWindows;

    private  int coefficientsToUse; // The number of fourier coefficients to use
    private  int currentPoint;  // Counts how many timepoints have passed in the current window
    private  int currentWindow;

    private  double RunningSumOfValues; // The sum of all the values during the current window
    private  double RunningSumOfSquares;
    private  Complex[] RunningDFT;
    private  double sumOfValues; // The sum of all the values during the current window
    private  double sumOfSquares;
    private String gridHashKey;
    private int indexCOEtoUSE;
    private String key;
    //synopsis
    private double mean;
    private double sigma;

    private LinkedList<Double> Sums;  // The sum of values for each basic window
    private LinkedList<Double> SquareSums;
    private LinkedList<Complex[]> DFTs; // The digests (zeta in paper) to update Fourier

    private Complex[] fourierCoefficients; // The Fourier Coefficients of the TimeSeries
    private Complex[] normalizedFourierCoefficients; // The normalized Fourier Coefficients

    public windowDFT( int bw, int sw, int coe, int ictu, String k) {
        key =k;
        indexCOEtoUSE = ictu;
        basicWindowSize = bw;
        slidingWindowSize =sw;
        if(bw!=0)
            NofWindows = sw/bw;
        currentPoint = 0;
        currentWindow = 0;
        coefficientsToUse =coe;
        gridHashKey="1";
        fourierCoefficients = new Complex[coefficientsToUse];
        normalizedFourierCoefficients = new Complex[coefficientsToUse];
        RunningDFT  = new Complex[coefficientsToUse];
        Sums = new LinkedList<>();
        SquareSums = new LinkedList<>();
        DFTs = new LinkedList<>();


        RunningSumOfValues = 0;
        RunningSumOfSquares = 0;

        for (int m = 0; m < coefficientsToUse; m++) {
            fourierCoefficients[m] = new Complex(0.0, 0.0);
            normalizedFourierCoefficients[m] = new Complex(0.0, 0.0);
            RunningDFT[m] = new Complex(0.0, 0.0);
        }
    }

    public void pushToValues(double newValue) {


        this.RunningSumOfValues += newValue;
        this.RunningSumOfSquares += newValue * newValue;
        computeNewDFTDigest(newValue);
        this.currentPoint++;

        if (  this.currentPoint >= this.basicWindowSize ) {

            this.currentPoint=0;
            this.mean = 0;
            this.sigma = 0;
            sumOfValues = sumOfValues + RunningSumOfValues;
            sumOfSquares =  sumOfSquares + RunningSumOfSquares;
            currentWindow++;
            Sums.add(RunningSumOfValues);   //add at the bottom of the list
            SquareSums.add(RunningSumOfSquares);
            DFTs.add(fourierCoefficients);
            double RemovedSum, RemovedsSum;

            if(currentWindow >= NofWindows + 1) {

                RemovedSum	= Sums.removeFirst();
                RemovedsSum = SquareSums.removeFirst();
                Complex[] temp = DFTs.removeFirst();

                sumOfValues = sumOfValues - RemovedSum;
                sumOfSquares =  sumOfSquares - RemovedsSum;

                this.mean = this.sumOfValues/this.slidingWindowSize;
                this.sigma = Math.sqrt((this.sumOfSquares/this.slidingWindowSize) - (this.mean * this.mean));


                for (int m = 1; m < coefficientsToUse; m++) {
                    RunningDFT[m] = RunningDFT[m].subtract(temp[m]);
                    normalizedFourierCoefficients[m] = RunningDFT[m].divide(sigma).divide(slidingWindowSize);
                    // normalizedFourierCoefficients[m] = normalizedFourierCoefficients[m].subtract(temp[m].divide(sigma).divide(basicWindowSize));
                    // fourierCoefficients[m] = fourierCoefficients[m].divide(sigma).divide(basicWindowSize);

                }
            }
            RunningSumOfValues = 0;
            RunningSumOfSquares = 0;
            currentPoint = 0;

            for (int k = 0; k < coefficientsToUse; k++) {
                fourierCoefficients[k] = new Complex(0.0, 0.0);
            }

        }    	//System.out.println("\""+normalizedFourierCoefficients[1].toString()+ ","+normalizedFourierCoefficients[2].toString() +"\"");


    }

    private void computeNewDFTDigest(double newValue) {
        for (int m = 0; m < coefficientsToUse; m++) {
            double exponent = 2 * Math.PI * m * (basicWindowSize - currentPoint) / (basicWindowSize);
            //double exponent = (2 * Math.PI * m * currentPoint) / (basicWindowSize);
            Complex exponentToTheE = new Complex(Math.cos(exponent), - Math.sin(exponent));
            fourierCoefficients[m] = fourierCoefficients[m].add(exponentToTheE.multiply(newValue));
            RunningDFT[m] = RunningDFT[m].add(exponentToTheE.multiply(newValue));
        }
    }


    public double getM() {
        double temp = 0.0;
        for (int i = 1; i < indexCOEtoUSE + 1; i++)
            temp = temp + getMagnitude(normalizedFourierCoefficients[i]);
        return temp;
    }



    private double getMagnitude(Complex coefficient) {
        return Math.sqrt(Math.pow(coefficient.getReal(), 2) + Math.pow(coefficient.getImaginary(), 2));
    }
    public Complex[] getNormalizedFourierCoefficients() {
        return normalizedFourierCoefficients;
    }


}