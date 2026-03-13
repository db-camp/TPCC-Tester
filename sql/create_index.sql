create index warehouse(w_id);
create index district(d_w_id, d_id);
create index customer(c_w_id, c_d_id, c_id);
create index new_orders(no_w_id, no_d_id, no_o_id);
create index orders(o_w_id, o_d_id, o_id);
create index order_line(ol_w_id, ol_d_id, ol_o_id, ol_number);
create index item(i_id);
create index stock(s_w_id, s_i_id);
