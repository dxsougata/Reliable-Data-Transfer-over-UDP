from src.common.packet import create_data_packet, Packet

# Create a packet
original = create_data_packet(1, "Hello from Selective Repeat")

print("Original packet:")
print("SEQ =", original.seq_num)
print("DATA =", original.data.decode())

# Serialize packet
serialized = original.serialize()

print("\nSerialized packet:")
print(serialized)

# Deserialize packet
received = Packet.deserialize(serialized)

print("\nReceived packet:")
print("SEQ =", received.seq_num)
print("DATA =", received.data.decode())

print("\nPacket transmission test successful!")