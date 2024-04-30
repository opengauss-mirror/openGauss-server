-- Enable packet loss fault, fault location is 11 and 22, with a 50% probability of packet loss
alter system set ss_fi_packet_loss_entries = '11,22';
alter system set ss_fi_packet_loss_prob = 50;

-- Disable packet loss fault.
alter system set ss_fi_packet_loss_entries = '';
