-- Enable packet loss fault, fault location is 33 and 44, with a 60% probability of packet loss
alter system set ss_fi_packet_loss_entries = '33,44';
alter system set ss_fi_packet_loss_prob = 60;

-- Disable packet loss fault.
alter system set ss_fi_packet_loss_entries = '';
