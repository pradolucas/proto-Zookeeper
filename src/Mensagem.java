import java.net.InetAddress;

public class Mensagem {
	private String key;
	private String value;
	private String type;
	private Long timestamp;
	private InetAddress ipS;
	private int portS;
	private InetAddress ipD;
	private int portD;

	public Mensagem(String key, String value, String type, long timestamp, InetAddress ipS, int portS, InetAddress ipD,
			int portD) {
		this.setKey(key);
		this.setValue(value);
		this.setType(type);
		this.setTimestamp(timestamp);
		this.setIpS(ipS);
		this.setPortS(portS);
		this.setIpD(ipD);
		this.setPortD(portD);
	}
	
	// Métodos getter e setter para os atributos da mensagem.
	// Esses métodos permitem obter e modificar os valores dos atributos, respeitando o encapsulamento.

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public InetAddress getIpS() {
		return ipS;
	}

	public void setIpS(InetAddress ipS) {
		this.ipS = ipS;
	}

	public int getPortS() {
		return portS;
	}

	public void setPortS(int portS) {
		this.portS = portS;
	}

	public InetAddress getIpD() {
		return ipD;
	}

	public void setIpD(InetAddress ipD) {
		this.ipD = ipD;
	}

	public int getPortD() {
		return portD;
	}

	public void setPortD(int portD) {
		this.portD = portD;
	}
}
