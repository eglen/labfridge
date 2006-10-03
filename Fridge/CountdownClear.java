import javax.swing.JTextArea;

public class CountdownClear extends Thread {

	//	The area to write the countdown to.
	JTextArea output;
	
	//countdown time (s)
	int countdownTime = 10;
	
	public CountdownClear(JTextArea output)
	{
		this.output=output;
	}
	
	public CountdownClear(JTextArea output, int countdownTime)
	{
		this.output=output;
		this.countdownTime = countdownTime;
	}
	
	public void run()
	{
		for (int i = countdownTime; i > 0; i--) {
			output.append(String.valueOf(i));
			//Subsecond countdown
			for(int j = 0; j < 4; j++)
			{
				output.append(".");
	            try {
	                sleep(250);
	            } catch (InterruptedException e) {}
			}
        }
        //Done counting.. wipe
		output.setText("");
		
	}
	
}
