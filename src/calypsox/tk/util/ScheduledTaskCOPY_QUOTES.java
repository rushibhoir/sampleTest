package calypsox.tk.util;

import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.Vector;

import com.calypso.tk.bo.Task;
import com.calypso.tk.core.Holiday;
import com.calypso.tk.core.JDate;
import com.calypso.tk.core.JDatetime;
import com.calypso.tk.core.Log;
import com.calypso.tk.core.Util;
import com.calypso.tk.event.PSConnection;
import com.calypso.tk.marketdata.QuoteSet;
import com.calypso.tk.marketdata.QuoteValue;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.service.LocalCache;
import com.calypso.tk.service.RemoteMarketData;
import com.calypso.tk.util.ScheduledTask;
import com.calypso.tk.util.TaskArray;


public class ScheduledTaskCOPY_QUOTES extends ScheduledTask {

	private String SOURCE_QUOTESET = "SOURCE QUOTESET";
	private String TARGET_QUOTESET = "TARGET QUOTESET";
	private String PRODUCT_TYPES = "Product types";
	private StringBuilder commentBuilder = new StringBuilder();
	private boolean error = false;
	private static Vector allProductTypes = null;
	private static Vector allQuoteSetNames = null;

	@Override
	public String getTaskInformation () {
		return "Copy quotes from source quoteset to target quoteset";
	}

	@Override
	public boolean process ( DSConnection dsconnection , PSConnection psconnection ) {
		System.out.println( "Starting ScheduledTask ScheduledTaskCOPY_QUOTES.." );
		copyQuotes( getValuationDatetime() , getAttribute( SOURCE_QUOTESET ) , getAttribute( TARGET_QUOTESET ) , getAttribute( PRODUCT_TYPES ) );
		System.out.println( "ScheduledTaskCOPY_QUOTES comments : " + commentBuilder );

		boolean flag = true;
		if (_publishB || _sendEmailB) flag = super.process( dsconnection , psconnection );
		TaskArray taskarray = new TaskArray();

		Task task = new Task();
		task.setObjectId( getId() );
		task.setEventClass( "Exception" );
		task.setNewDatetime( getValuationDatetime() );
		task.setUnderProcessingDatetime( new JDatetime() );
		task.setUndoTradeDatetime( getUndoDatetime() );
		task.setDatetime( getDatetime() );
		task.setPriority( 1 );
		task.setId( 0L );
		task.setStatus( 0 );
		task.setEventType( "EX_INFORMATION" );
		task.setSource( getType() );
		task.setCompletedDatetime( new JDatetime() );
		if (!error && _executeB && flag) {
			task.setEventType( "EX_VALUATION_PROCESS_SUCCESS" );
			task.setComment( ( new StringBuilder() ).append( toString() ).append( " : Successfully completed." ).toString() );
		} else {
			task.setEventType( "EX_VALUATION_PROCESS_FAILURE" );
			task.setComment( ( new StringBuilder() ).append( toString() ).append( " : " ).append( "Failed : " ).append( getComments() ).toString() );
		}
		if (Log.isCategoryLogged( "ScheduledTaskCOPY_QUOTES" )) Log.debug( "ScheduledTaskCOPY_QUOTES" , task.getComment() );

		taskarray.add( task );

		try {
			getReadWriteDS( dsconnection ).getRemoteBO().saveAndPublishTasks( taskarray , 0 , null );
		} catch (Exception exception) {
			if (Log.isCategoryLogged( "ScheduledTaskCOPY_QUOTES" )) Log.error( "ScheduledTaskCOPY_QUOTES" , exception );
		}
		if (!error)
			return true;
		else
			return false;
	}

	@SuppressWarnings("unchecked")
	public Vector getDomainAttributes () {
		Vector vector = new Vector();
		vector.addElement( SOURCE_QUOTESET );
		vector.addElement( TARGET_QUOTESET );
		vector.addElement( PRODUCT_TYPES );
		return vector;
	}

	@SuppressWarnings("unchecked")
	public Vector getAttributeDomain ( String s , Hashtable hashtable ) {
		if (s.equals( SOURCE_QUOTESET ) || s.equals( TARGET_QUOTESET )) {
			if (allQuoteSetNames == null) {
				try {
					allQuoteSetNames = DSConnection.getDefault().getRemoteMarketData().getQuoteSetNames();
				} catch (RemoteException e) {
					System.out.println( e.getMessage() );
				}
			}
			return allQuoteSetNames;
		} else {
			return new Vector();
		}
	}

	@SuppressWarnings("unchecked")
	public boolean isValidInput ( Vector vector ) {
		boolean flag = super.isValidInput( vector );

		String s = getAttribute( SOURCE_QUOTESET );
		if (Util.isEmptyString( s )) {
			flag = false;
			vector.add( ( new StringBuilder() ).append( SOURCE_QUOTESET ).append( " is mandatory" ).toString() );
		}

		String s1 = getAttribute( TARGET_QUOTESET );
		if (Util.isEmptyString( s1 )) {
			flag = false;
			vector.add( ( new StringBuilder() ).append( TARGET_QUOTESET ).append( " is mandatory" ).toString() );
		}

		String s2 = getAttribute( PRODUCT_TYPES );
		if (Util.isEmptyString( s2 )) {
			flag = false;
			vector.add( ( new StringBuilder() ).append( PRODUCT_TYPES ).append( " is mandatory" ).toString() );
		} else {
			if (allProductTypes == null) {
				allProductTypes = LocalCache.getDomainValues( DSConnection.getDefault() , "productType" );
			}
			Vector v = Util.string2Vector( s2 );
			for ( int i = 0 ; i < v.size() ; i++ ) {
				if (!allProductTypes.contains( v.elementAt( i ).toString().trim() )) {
					vector.add( ( new StringBuilder() ).append( "Product type " ).append( v.elementAt( i ) ).append( " is invalid" ).toString() );
					flag = false;
					break;
				}
			}
		}
		if (!getExecuteB()) {
			flag = false;
			vector.add( ( new StringBuilder() ).append( "Set Execute check to true" ) );
		}
		return flag;
	}

	/**
	 * Copies quotes from source quote set to target quote set for given set of
	 * products.
	 * 
	 * @param valDateTime
	 * @param sourceQuoteSet
	 * @param targetQuoteSet
	 * @param prodList
	 */
	private void copyQuotes ( JDatetime valDateTime , String sourceQuoteSet , String targetQuoteSet , String prodList ) {
		//business logic goes here
			
	}

	/**
	 * Get the comments/errors while copying the quotes.
	 */
	private Object getComments () {
		return commentBuilder.toString();
	}

}
