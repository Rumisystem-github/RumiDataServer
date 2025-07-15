package su.rumishistem.rumidataserver.MODULE;

import java.util.*;

public class LRUCache extends LinkedHashMap<String, byte[]> {
	private final int MaxEntries;

	public LRUCache(int MaxEntries) {
		super(16, 0.75f, true);
		this.MaxEntries = MaxEntries;
	}

	@Override
	protected boolean removeEldestEntry(Map.Entry<String, byte[]> Eldest) {
		return size() > MaxEntries;
	}
}
