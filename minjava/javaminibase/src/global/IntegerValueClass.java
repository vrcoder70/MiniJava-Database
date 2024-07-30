package global;

public class IntegerValueClass extends ValueClass
{
	public int classValue = -1;
	
	public IntegerValueClass()
	{
		valueType = AttrType.attrInteger;
		valueLength = 4; //shouldn't be used in this implementation, but all are bytes
	}
	
	public IntegerValueClass( int value )
	{
		classValue = value;
		valueType = AttrType.attrInteger;
		valueLength = 4; //shouldn't be used in this implementation, but all are bytes
	}
	
	public byte[] getClassValue()
		throws java.io.IOException
	{
		byte[] data = new byte[GlobalConst.MAX_NAME];
		Convert.setIntValue(classValue, 0, data);
		return data;
	}
}
