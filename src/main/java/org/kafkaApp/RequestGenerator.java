package org.kafkaApp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.kafkaApp.Structure.RequestStructure;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestGenerator {
    private AtomicInteger lastRequestId = new AtomicInteger(0);
    private AtomicInteger lastUid = new AtomicInteger(1000); // Initialize uid to 1000

    private static String PATHFigure1 = "C:\\dataset\\RequestTotal\\Dataset100StreamsThroughVsWorkers";
    private static String PATHFigure2 = "C:\\dataset\\RequestTotal\\Parallilism9ThreadsThroughVsStreams";
    private static String PATHFigure3 = "C:\\dataset\\RequestTotal\\CommunicationCost100Streams";
    public RequestStructure createRequest(String streamID, int synopsisID, Object[] param, int noOfP,String datasetKey) {
        RequestStructure request = new RequestStructure();
        request.setStreamID(streamID);
        request.setSynopsisID(synopsisID);
        request.setRequestID(lastRequestId.incrementAndGet());
        request.setDataSetKey(datasetKey);
        request.setParam(param);
        request.setNoOfP(noOfP);
        request.setUid(lastUid.incrementAndGet()); // Increment uid for each new request
        return request;
    }
    public RequestStructure createQueryRequest(String streamID, int synopsisID, Object[] param, int noOfP,String datasetKey) {
        RequestStructure request = new RequestStructure();
        request.setStreamID(streamID);
        request.setSynopsisID(synopsisID);
        request.setRequestID(lastRequestId.get());
        request.setDataSetKey(datasetKey);
        request.setParam(param);
        request.setNoOfP(noOfP);
        request.setUid(lastUid.get());
        return request;
    }

    public void writeRequestsToJsonFile(List<RequestStructure> requests, String filePath) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        try (PrintWriter out = new PrintWriter(new FileWriter(filePath, false))) { // Overwrite the file
            String jsonRequests = mapper.writeValueAsString(requests);
            out.println(jsonRequests);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /*public static void main(String[] args) {
        RequestGenerator generator = new RequestGenerator();
        int nOfP = 6;

        Object[] param1 = {1,"volume","Queryable","Continues",0.01, 0.99, 12345};
        Object[] param2 = {6.18173,"price","Queryable","Continues",1,500,2000,6};
        Object[] param3 = {0.0,"price","Queryable","Continues",0.01};


        String[] streamIds = {
                "EURTRY", "XAUUSD", "AUDCHF", "AUDCAD", "AUDNZD", "AUDJPY",
                "AUDUSD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD",
                "EURNOK", "EURCHF", "EURGBP", "EURHUF", "EURJPY", "EURMXN",
                "EURNZD", "EURPLN"
        };

        List<RequestStructure> requests = new ArrayList<>();
        for (int i = 0; i < streamIds.length; i++) {
            String streamId = streamIds[i];
            if (i < 7) {
                RequestStructure request = generator.createRequest(streamId, 1, param1, nOfP);
                requests.add(request);
            } else if (i < 14) {
                RequestStructure request = generator.createRequest(streamId, 4, param2, nOfP);
                requests.add(request);
            } else {
                RequestStructure request = generator.createRequest(streamId, 2, param3, nOfP);
                requests.add(request);
            }
        }

        generator.writeRequestsToJsonFile(requests, "C:\\dataset\\RequestCases\\CommunicationCost\\requests20StreamsParallel6ForComunicationCostVM3Query2.json");
    }*/

    public static void main(String[] args) {
        RequestGenerator generator = new RequestGenerator();
        int synopsisId = 0;
        int nOfP = 18;
        Object[] param = {"RawCountMin","price","NotQueryable",0.002, 0.99, 12345};
        //Object[] param = {"HyperLogLog","price","NotQueryable",0.002};
        //Object[] param = {"BloomFitler","price","NotQueryable",150000,0.01};
        //Object[] param = {"DFT","price","NotQueryable",1,500,2000,8};
        //Object[] param = {"LossyCounting","price","NotQueryable",0.001};
        //Object[] param = {"StickySampling","price","NotQueryable",0.001,0.0001,0.001};
        //Object[] param = {"AMSSketch","price","NotQueryable",10,10000};
        //Object[] param = {"GKQuantiles","price","NotQueryable",0.01};

       //Object[] param = {0.69196,"price","Queryable","Continues",0.002, 0.99, 12345};
        //Object[] param = {0.69196,"price","Queryable","Continues",0.002};
        //Object[] param = {0.69196,"price","Queryable","Continues",150000,0.01};
        //Object[] param = {0.69196,"price","Queryable","Continues",1,500,2000,8};
        //Object[] param = {new Object[]{0.69196,"nofrequentItems"},"price","Queryable","Continues",0.001};
       // Object[] param = {new Object[]{0.001,"frequentItems"},"price","Queryable","Continues",0.001};
        //Object[] param = {new Object[]{0.001,"frequentItems"},"price","Queryable","Continues",0.001,0.0001,0.001};
        //Object[] param = {new Object[]{0.95272,"nofrequentItems"},"price","Queryable","Continues",0.001,0.0001,0.001};
        //Object[] param = {new Object[]{100,"isFrequent"},"price","Queryable","Continues",0.001,0.0001,0.001};
        //Object[] param = {new Object[]{0.95272,"countEstimation"},"price","Queryable","Continues",10,10000};
        //Object[] param = {new Object[]{0,"L2norm"},"price","Queryable","Continues",10,10000};
        //Object[] param = {0.5,"price","Queryable","Continues",0.01};
        String[] streamIds = {
                //forex 49
                "EURTRY", "XAUUSD", "AUDCHF", "AUDCAD", "AUDNZD", "AUDJPY",
                "AUDUSD", "CADCHF", "CADJPY", "CHFJPY", "EURAUD", "EURCAD",
                "EURNOK", "EURCHF", "EURGBP", "EURHUF", "EURJPY", "EURMXN",
                "EURNZD", "EURPLN", "EURSEK", "EURUSD", "EURZAR", "GBPAUD",
                "GBPCAD", "GBPCHF", "GBPJPY", "GBPNZD", "GBPUSD", "GBPZAR",
                //30

                "NZDCAD", "NZDCHF", "NZDJPY", "NZDUSD", "USDCAD", "USDCHF",
                "USDCZK", "USDDKK", "USDHKD", "USDHUF", "USDJPY", "USDMXN",
                "USDNOK", "USDPLN", "USDSEK", "USDSGD", "USDTRY", "USDZAR",
                "XAGUSD",
                //crypto 9
                "BCHUSD", "BTCEUR", "BTCUSD", "DSHUSD", "ETHEUR", "ETHUSD",
                "LTCEUR", "LTCUSD", "NEOUSD",
                //future 18
                "C", "DJ", //60

                "EF", "FCE", "FDAX", "FESX", "FTI", "HG", "HUNG#",
                "ITA40#", "KC", "ND", "NG", "SP", "S", "W20#", "W", "Z",

                //Stocks FranceEURONEXT 83
                "ACCP", "AIR", "AIRF", "AIRP", "AKE", "ALSO", "ALTT", "ATOS", "AXAF", "BICP",
                "BNPP", "BOLL", "BOUY", "BVI", "CAGR", "CAPP", "CARR", "CASP", "CITT", "CNAT",
                "CNPP", "DANO", "DAST", "DIOR", //100

               /* "EDEN", "EDF", "ENGIE", "EPED", "ERMT", "ESSI",
                "EXHO", "FDR", "FOUG", "GENC", "GEPH", "GETP", "GFCP", "HRMS", "ICAD", "ILD",
                "INGC", "ISOS", "JCDX", "LAGA", "LEGD", "LOIM", "LVMH", "MERY", "MICP", "NEXS",
                "NPOS", "ORAN", "OREP", "ORP", "PERP", "PEUP", "PLOF", "PRTP", "PUBP", "RCOP",
                "RENA", "ROCH", "RXL", "SAF", "SASY", "SCHN", "SCOR", "SESFd", "SEVI", "SGEF",
                "SGOB", "SOGN", "SOPR", "STM", //150

                "TCFP", "TCH", "TFFP", "TOTF", "UBIP", "VCTP",
                "VIE", "VIV", "VLLP",

                //Stocks GermanyXETRA 77
                "ADSGn", "AIXGn", "ALVG", "ARLG", "BASFn", "BAYGn", "BEIG", "BIONn", "BMWG", "BNRGn",
                "BOSSn", "CBKG", "CECG", "CONG", "DAIGn", "DB1Gn", "DBKGn", "DEZG", "DLGS", "DPWGn",
                "DTEGn", "EONGn", "FMEG", "FNTGn", "FPEG_p", "FRAG", "FREG", "G1AG", "GBFG", "GILG",
                "HDDG", "HEIG", "HNKG_p", "HNRGn", "HOTG", "IFXGn", "KCOGn", "KRNG", "LEOGn", "LHAG",
                "LING", "LXSG", "MANG", "MORG", "MRCG", "MTXGn", "MUVGn", "NAFG", "NDXG", "PSHG_p",
                "PSMGn", "PV", "QGEN", "QSCG", "RHKG", "RHMG", "RWEG", "SAPG", "SDFGn", "SGCG",
                "SIEGn", "SMHNn", "SOWG", "SPRGn", "SY1G", "SZGG", "SZUG", "TKAG", "TUIGn", "UNI",
                "UTDI", //230

               /* "VOWG", "VOWG_p", "WCHG", "WDIG", "WING", "ZILGn",

                //Stocks NetherlandsAEX 18
                "AD", "AEGN", "AKZO", "ASML", "BAMN", "BOSN", "DSMN", "FUGRc", "HEIN", "ING",
                "KPN", "PHG", "RAND", "RELX", "TOM2", "UNc", "WEHA", "WLSNc",

                //Stocks SpainMAD 31
                "ACS", "ACX", "AMA", "ANA", "BBVA", "BKT", "CABK", "DIDA", "EBRO", "ELE",
                "ENAG", "FER", "GAM", "GRLS", "IBE", "ICAG", "IDR", "ITX", "MAP", "MRL",
                "MTS", "OHL", "REE", "REP", "SABE", "SAN", "SCYR", "TEF", "TL5", "TRE",
                "VIS",
                //United Kingdom 132
                "AAL", "ACAA", "ADML", "AGGK", "AHT", "ANTO", "ASHM", "AV", "AVV", "AZN", "BAB", "BAES", "BALF", "BARC", "BATS",
                "BDEV", "BKGH", "BLND", "BNZL", "BP", "BRBY", "BT", "BTG", "BVIC", "BVS", "BWY", "CAPCC", "CCL", "CEY", "CNA",
                "CNE", "COB", "CPG", "CPI", "CRDA", "DCC", "DEB", "DGE", "DLGD", "DOM", "EMG", "ESNT", "EXPN", "EZJ", "FGP",
                "FRES", "GFS", "GLEN", "GNC", "GNK", "GSK", "HAYS", "HICL", "HIK", "HLMA", "HMSO", "HRGV", "HSBA", "HWDN", "ICAG",
                "ICP", "IGG", "IHG", "III", "IMI", "INCH", "INDV", "INF", "INTUP", "INVP", "IPF", "ITRK", "ITV", "JMAT", "JUP",
                "KAZ", "KGF", "LAND", "LGEN", "LLOY", "LMI", "LRE", "LSE", "MKS", "MRW", "NXT", "OCDO", "PARA", "PFC", "PNN",
                "PRU", "PSN", "PSON", "QQ", "RDSa", "REL", "RIO", "RMG", "RMV", "RSA", "RTO", "SBRY", "SDR", "SGE", "SHI", "SHP",
                "SJP", "SL", "SMIN", "SMWH", "SN", "SPD", "SPI", "SPX", "SRP", "SSE", "SSPG", "STAN", "SVT", "SXS", "TALK",
                "TEM", "TLW", "TSCO", "TW", "ULVR", "UU", "VOD", "WEIR", "WMH", "WPP", "WTB"*/

        };//



        System.out.println(streamIds.length);

        List<RequestStructure> requests = new ArrayList<>();
        int count=0;
        String datasetKey = null;


        for (String streamId : streamIds) {

            if(count==34){
                //synopsisId = 4;
                param = new Object[]{"RawDFT","price","NotQueryable",1,500,2000,8};
            }
            if(count==67){
                //synopsisId = 2;
                param = new Object[]{"RawHyperLogLog", "price", "NotQueryable", 0.002};
            }

            if(count<49)
                datasetKey="Forex";
            else if(count< 58)
                datasetKey="Cryptos";
            else if(count<76)
                datasetKey="Futures";
            else if(count<159)
                datasetKey="Stocks FranceEURONEXT";
            else if(count<236)
                datasetKey="Stocks GermanyXETRA";
            else if(count<254)
                datasetKey="Stocks NetherlandsAEX";
            else if(count<285)
                datasetKey="Stocks SpainMAD";
            else if(count<417)
                datasetKey="Stocks United KingdomLSE";

            count++;
            RequestStructure request = generator.createRequest(streamId, synopsisId, param, nOfP,datasetKey);
            //RequestStructure request2 = generator.createQueryRequest(streamId, synopsisId, param2, nOfP,datasetKey);
            requests.add(request);
            //requests.add(request2);
        }
        String synopsisName=null;
        if (synopsisId == 1) {
            synopsisName="CountMin";
        } else if (synopsisId == 2) {
            synopsisName="HyperLogLog";
        } else if (synopsisId == 3) {
            synopsisName="BloomFilter";

        }
        else if (synopsisId == 4) {
            synopsisName="DFT";
        }
        else if (synopsisId == 5) {
            synopsisName="LossyCount";
        }
        else if (synopsisId == 6) {
            synopsisName="StickySampling";
        }
        else if (synopsisId == 7) {
            synopsisName="AmmsSketch";
        }
        else if (synopsisId == 8) {
            synopsisName="Gkquantilies";
        }

        generator.writeRequestsToJsonFile(requests, PATHFigure3+"\\"+"requestsCommunicationCost100StreamsRawData"+nOfP+"Parallilism"+".json"); //  100Streams
    //C:\dataset\RequestCases\QueryRequestToCheckResult
        //generator.writeRequestsToJsonFile(requests, PATHFigure3+"\\"+"requests"+synopsisName+"9Parallilism"+streamIds.length+"Streams"+".json"); //  100Streams
        //generator.writeRequestsToJsonFile(requests, PATHFigure3+"\\"+"requests"+synopsisName+"9Parallilism"+streamIds.length+"StreamsCON"+".json"); //  100Streams

        //generator.writeRequestsToJsonFile(requests, PATHFigure2+"\\"+"requests"+synopsisName+"9Parallilism"+nOfP+".json"); //  100Streams
    }
}
