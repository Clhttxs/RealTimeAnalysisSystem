import Constant.StopGracefullyConstant;
import RealTimeUtils.StopGracefullyUtil;


public class StopGracefully {

   /* @org.junit.Test
    public void test() throws InterruptedException {

    }*/

    public static void main(String[] args) {
     //StopGracefullyUtil.stopGracefully(StopGracefullyConstant.STOPSTARTLOG);
     //StopGracefullyUtil.stopGracefully(StopGracefullyConstant.STOPLOGDIVERSION);
     // StopGracefullyUtil.stopGracefully(StopGracefullyConstant.STOPGVM);
     // StopGracefullyUtil.stopGracefully(StopGracefullyConstant.StopAlert);
        StopGracefullyUtil.stopGracefully(StopGracefullyConstant.StopSaleDetail);
    }
}