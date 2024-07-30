
package global;
import java.lang.*;

public class StringValueClass extends ValueClass
{
	public String classValue = "";
	
	public StringValueClass()
	{
		valueType = AttrType.attrString;
		valueLength = 0;
	}
	
	public StringValueClass( String value )
	{
		classValue = value;
		valueType = AttrType.attrString;
		valueLength = classValue.length();
	}
	
	public byte[] getClassValue()
		throws java.io.IOException
	{
		byte[] data = new byte[GlobalConst.MAX_NAME];
		Convert.setStrValue(classValue, 0, data);
		return data;
	}
}
