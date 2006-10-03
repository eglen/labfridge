
public class KeyTimer extends Thread {

	//The fridge to notify when the time counts down (recursive relationship..nice)
	Fridge fridge;
	
	//time to wait (ms)
	int waitTime = 250;
	
	public KeyTimer (Fridge fridge)
	{
		this.fridge = fridge;
	}
	
	public void run()
	{
		try {
			sleep(waitTime);
//			Once sleep is finished, tell the fridge
			fridge.tryTransaction();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
