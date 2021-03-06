package wdsr.exercise4;

import java.io.Serializable;

public class VolumeAlert implements Serializable {
	private static final long serialVersionUID = -814547112765727554L;
	
	private long timestamp;
	private String stock;
	private long floatingVolume;
	
	public VolumeAlert(long timestamp, String stock, long floatingVolume) {
		this.timestamp = timestamp;
		this.stock = stock;
		this.floatingVolume = floatingVolume;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getStock() {
		return stock;
	}

	public long getFloatingVolume() {
		return floatingVolume;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (floatingVolume ^ (floatingVolume >>> 32));
		result = prime * result + ((stock == null) ? 0 : stock.hashCode());
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VolumeAlert other = (VolumeAlert) obj;
		if (floatingVolume != other.floatingVolume)
			return false;
		if (stock == null) {
			if (other.stock != null)
				return false;
		} else if (!stock.equals(other.stock))
			return false;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}
}
