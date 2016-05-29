<h1>Predicting stock portfolio losses using Monte Carlo simulation in Spark</h1>
<h2>Summary</h2>
<p>
Have you ever asked yourself: what is the most money my
stock holdings could lose in a single day? If you own stock through a 401k, a personal trading account, or employer provided stock options then you should absolutely ask yourself this question.  Now think about how to answer it.  Your first guess maybe to pick a random number, say 20%, and assume that is the worst case scenario.  While simple, this is likely to be wildly inaccurate and certainly doesn’t take into account the positive impacts of a diversified portfolio.  Surprisingly, a good estimate is hard to calculate.  Luckily, financial institutions have to do this for their stock portfolios (called Value at Risk (VaR)), and we can apply their methods to individual portfolios.  In this article we will run a Monte Carlo simulation using real trading data to try to quantify what can happen to your portfolio.  You should now go to your broker website (Fidelity, E*Trade, etc...) and get a list of stocks that you own and the % that each holding represents of the total portfolio.
</p>
<h2>How it works</h2>
<p>
The Monte Carlo method is one that uses repeated sampling to predict a result.  As a real-world example, think about how you might predict where your friend is aiming while throwing a dart at a dart board.  If you were following the Monte Carlo method, you'd ask your friend to throw a 100 darts with the same aim, and then you'd make a prediction based on the largest cluster of darts.  To predict stock returns we are going to pick 1,000,000 previous trading dates at random and see what happened to on those dates.  The end result is going to be some aggregation of those results.&nbsp;
</p>
<p>
	 We will download historical stock trading data from Yahoo Finance and store them into HDFS.  Then we will create a table in Spark like the below and pick a million random dates from it.
</p>
<table>
<thead>
<tr>
	<td style="text-align: center;">
	</td>
	<td style="text-align: center;">
		GS
	</td>
	<td style="text-align: center;">
		AAPL
	</td>
	<td style="text-align: center;">
		GE
	</td>
	<td style="text-align: center;">
		OIL
	</td>
</tr>
</thead>
<tbody>
<tr>
	<td style="text-align: center;">
		2015-01-05
	</td>
	<td style="text-align: center;">
		-3.12%
	</td>
	<td style="text-align: center;">
		-2.81%
	</td>
	<td style="text-align: center;">
		-1.83%
	</td>
	<td style="text-align: center;">
		-6.06%
	</td>
</tr>
<tr>
	<td style="text-align: center;">
		2015-01-06
	</td>
	<td style="text-align: center;">
		-2.02%
	</td>
	<td style="text-align: center;">
		-0.01%
	</td>
	<td style="text-align: center;">
		-2.16%
	</td>
	<td style="text-align: center;">
		-4.27%
	</td>
</tr>
<tr>
	<td style="text-align: center;">
		2015-01-07
	</td>
	<td style="text-align: center;">
		+1.48%
	</td>
	<td style="text-align: center;">
		+1.40%
	</td>
	<td style="text-align: center;">
		+0.04%
	</td>
	<td style="text-align: center;">
		+1.91%
	</td>
</tr>
<tr>
	<td style="text-align: center;">
		2015-01-08
	</td>
	<td style="text-align: center;">
		+1.59%
	</td>
	<td style="text-align: center;">
		+3.83%
	</td>
	<td style="text-align: center;">
		+1.21%
	</td>
	<td style="text-align: center;">
		+1.07%
	</td>
</tr>
<tr>
	<td colspan="5" style="text-align: center;">
		<em>Table 1: percent change per day by stock symbol
		</em>
	</td>
</tr>
</tbody>
</table>
<p>
	 We combine the column values with the same proportions as your trading account.  For example, if on Jan 5th 2015 you equaliy invested all of your money in GS, AAPL, GE, and OIL then you would have lost
</p>
<pre>
% loss on 2015-01-05 = -3.12*(1/4) - 2.81*(1/4) - 1.83*(1/4) - 6.06*(1/4)
</pre>
<p>
	At the end of a Monte Carlo simulation we have 1,000,000 values that represent the possible gains and losses.  We sort the results and take the 5th percentile, 50th percentile, and 95th percentile to represent the worst-case, average case, and best case scenarios.
</p>
<p>
	When you run the below, you'll see this in the output
</p>
<pre>
In a single day, this is what could happen to your stock holdings if you have $1000 invested
                                $       %
               worst case     -33   -3.33%
     most likely scenario      -1   -0.14%
                best case      23    2.28%
</pre>
<p>
	The&nbsp;
	<a href="https://github.com/vzlatkin/MonteCarloVarUsingRealData">code on GitHub</a> also has examples of:
</p>
<ol>
	<li>How to use Java 8&nbsp;Lambda Expressions</li>
	<li>Executing Hive SQL with Spark RDD objects</li>
	<li>Unit testing Spark code with&nbsp;<a href="https://github.com/sakserv/hadoop-mini-clusters">hadoop-mini-clusters</a></li>
</ol>
<h2>Detailed Step-by-step guide</h2>
<h3>1. Download and install the HDP Sandbox</h3>
<p>
	Download the latest (2.4 as of this writing) HDP Sandbox
	<a href="http://hortonworks.com/products/hortonworks-sandbox/#install.">here</a>. Import it into VMware or VirtualBox, start the instance, and update the DNS entry on your host machine to point to the new instance’s IP. On Mac, edit
	<em>/etc/hosts</em>, on Windows, edit <em>%systemroot%\system32\drivers\etc\</em> as administrator and add a line similar to the below:
</p>
<pre>
192.168.56.102  sandbox sandbox.hortonworks.com
</pre>
<h3>2. Download code and prerequisites</h3>
<p>
Log into the Sandbox and execute:
</p>
<pre>
useradd guest
su - hdfs -c "hdfs dfs -mkdir  /user/guest; hdfs dfs -chown guest:hdfs /user/guest; "
yum install -y java-1.8.0-openjdk-devel.x86_64
#update-alternatives --install /usr/lib/jvm/java java_sdk /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.91-0.b14.el6_7.x86_64  100
cd /tmp
git clone https://github.com/vzlatkin/MonteCarloVarUsingRealData.git
</pre>
<h3>3. Update list of stocks that you own</h3>
<p>
Update 
	<em>companies_list.txt</em> with the list of companies that you own in your stock portfolio and either the portfolio weight (as %/100) or the dollar amount. &nbsp;You should be able to get this information from your broker's website (Fidelity, Scottrade, etc...). &nbsp;Take out any extra commas (,) if you are copying and pasting from the web. &nbsp;The provided sample looks like this:
</p>
<pre>
Symbol,Weight or dollar amount (must include $)
GE,$250
AAPL,$250
GS,$250
OIL,$250
</pre>
<h3>4. Download historical trading data for the stocks you own</h3>
<p>
Execute:
</p>
<pre>
cd /tmp/MonteCarloVarUsingRealData/
/bin/bash downloadHistoricalData.sh
# Downloading historical data for GE
# Downloading historical data for AAPL
# Downloading historical data for GS
# Downloading historical data for OIL
# Saved to /tmp/stockData/
</pre>
<h3>5. Run the MonteCarlo simulation</h3>
<p>
	Execute:
</p>
<pre>
su - guest -c " /usr/hdp/current/spark-client/bin/spark-submit --class com.hortonworks.example.Main --master yarn-client  --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 --queue default   /tmp/MonteCarloVarUsingRealData/target/monte-carlo-var-1.0-SNAPSHOT.jar  hdfs:///tmp/stockData/companies_list.txt hdfs:///tmp/stockData/*.csv"
</pre>
<h2>Interpreting the Results</h2>
<p>
Below is the result of a sample portfolio that has $1,000 invested equally between Apple, GE, Goldman Sachs, and an ETF that holds crude oil.  It says that with 95% certainty, the most that the portfolio can go down in a single day is $33.  In addition, there is a 5% chance that the portfolio will gain $23 in a single day. &nbsp;Most of the time, the portfolio will lose $1 per day.
</p>
<pre>
In a single day, this is what could happen to your stock holdings if you have $1000 invested
                                $       %
               worst case     -33   -3.33%
     most likely scenario      -1   -0.14%
                best case      23    2.28%
</pre>