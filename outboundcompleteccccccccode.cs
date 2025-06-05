static IEnumerable<AudiViewModel> GetInOutbounding(IEnumerable<AudiViewModel> result, IEnumerable<InvoiceViewModel> result45, IEnumerable<InvoiceViewModel> resultpalletid, string conum, string whnum, int inout)
        {
            List<AudiViewModel> mylistfinal = new List<AudiViewModel>();
            List<AudiViewModel> mylistfinal5 = new List<AudiViewModel>();
            List<AudiViewModel> mylistfinal2 = new List<AudiViewModel>();
            List<AudiViewModel> wresult = new List<AudiViewModel>();
            var wsettingyfind = db1.SettingRates.Join(db1.Ratebys,
                                       so => new { so.RatebyId },
                                     to => new { to.RatebyId },
                                     (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.so.KindId == inout)
                                     .Select(z => z.to).ToList();
            var result2666 = wsettingyfind.FirstOrDefault(x => x.CalculRate == 2666);
            var resultgroup = (from c in result
                               group c by new { c.abs_num, c.po_number, c.po_suffix } into g
                               select g).ToList();
            var oo56 = (from c in result orderby c.abs_num select c).GroupBy(g => new { abs_num = g.abs_num.ToUpper(), g.lot, g.po_number, g.po_suffix }).ToList();
            var txt22 = oo56.Where(x => x.Key.po_number == "6776956").ToList();

            var resnew = (from c in result orderby c.abs_num select c).GroupBy(g => new { abs_num = g.abs_num.ToUpper(), g.po_number, g.po_suffix }).Select(x => x.FirstOrDefault());
            if (result2666 != null)
            {
                var resnu = (from c in result
                             group c by new { abs_num = c.abs_num.ToUpper(), c.lot, c.po_number, c.po_suffix } into g
                             select new {abs_num=g.Key.abs_num.ToUpper(),g.Key.lot, g.Key.po_number, g.Key.po_suffix, quantity= g.Sum(o => o.quantity) }).ToList();

                resnew = (from c in result orderby c.abs_num select c).GroupBy(g => new { abs_num = g.abs_num.ToUpper(), g.lot, g.po_number, g.po_suffix }).Select(x => x.FirstOrDefault());

            }

            
            List<AudiViewModel> mylist = new List<AudiViewModel>();
            List<InvoiceViewModel> mylistinvoice = new List<InvoiceViewModel>();
            List<InvoiceViewModel> mylistpaletid = new List<InvoiceViewModel>();
            mylistinvoice = result45.ToList();
            mylistpaletid = resultpalletid.ToList();
            mylist = resnew.ToList();
            wresult = result.ToList();
            var reee = wresult.OrderBy(x => x.abs_num).ToList();
            var conti = 0;
            foreach (var item in reee)
            {

                conti = 0;
                var resu = mylist.Where(x => x.abs_num == item.abs_num && x.po_number == item.po_number && x.po_suffix == item.po_suffix);
                foreach (var item5 in resu)
                {

                    if (item5.po_suffix == item.po_suffix && item5.po_number == item.po_number && item5.abs_num == item.abs_num && item5.po_line == item.po_line)
                    {
                        conti = 1;
                        break;

                    }

                }
                if (conti == 0)
                {
                    AudiViewModel red = new AudiViewModel();
                    red.abs_num = item.abs_num;
                    red.quantity = item.quantity;
                    red.date_time = item.date_time;
                    red.item_qty = item.item_qty;
                    red.po_number = item.po_number;
                    red.po_suffix = item.po_suffix;
                    red.trans_type = item.trans_type;
                    red.item_num = item.item_num;
                    red.lot = item.lot;
                    red.uom = item.uom;
                    red.po_line = item.po_line;
                    red.co_num = item.co_num;
                    red.wh_num = item.wh_num;
                    red.act_qty = item.act_qty;
                    red.trans_num = item.trans_num;
                    red.date_time2 = item.date_time2;
                    red.row_status = item.row_status;
                    red.item_type = item.item_type;
                    red.emp_num = item.emp_num;
                    red.result_msg = item.result_msg;
                    red.case_qtyitem = item.case_qtyitem;
                    red.box_qtyitem = item.box_qtyitem;
                    red.bin_to = item.bin_to;
                    mylist.Add(red);
                }
            }
            //************************************

            var wsettingy = db1.SettingRates.Join(db1.RateDescs,
                                         so => new { so.RateDescId },
                                       to => new { to.RateDescId },
                                       (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.RateDescCountPo != 1 && x.to.RateDescInvoice == 0 && x.so.KindId == inout)
                                       .Select(z => z.so).ToList();
            var wsetting = (from c in wsettingy
                            join k in db1.RateDescs on c.RateDescId equals k.RateDescId
                            orderby k.Sorttable
                            select c).ToList();
            //****************************************************************************
            //****************************** Calcul Rate *********************************
            var groupPonew = (from c in mylist

                              group c by new { c.po_number, c.po_suffix, abs_num = c.abs_num.ToUpper() } into d

                              select new
                              {
                                  Remotekey = d.Key.po_number,
                                  Rabsnum = d.Key.abs_num.ToUpper(),
                                  posuffix = d.Key.po_suffix,
                                  Total = d.Sum(x => x.quantity)

                              }).OrderBy(x => x.Remotekey).ThenBy(x => x.posuffix).ToList();
            var groupPonew78 = (from c in mylist

                              group c by new { c.po_number, c.po_suffix, abs_num = c.abs_num.ToUpper(), c.lot,c.bin_num, c.pallet_id } into d

                              select new
                              {
                                  Remotekey = d.Key.po_number,
                                  wlot = d.Key.lot,
                                  Rabsnum = d.Key.abs_num.ToUpper(),
                                  posuffix = d.Key.po_suffix,
                                  wbinnum = d.Key.bin_num,
                                  wpalletid=d.Key.pallet_id,
                                  Total = d.Sum(x => x.quantity)

                              }).OrderBy(x => x.Remotekey).ThenBy(x => x.posuffix).ToList();
            var groupPonew88 = (from c in mylist

                                group c by new { c.po_number, c.po_suffix } into d

                                select new
                                {
                                    Remotekey = d.Key.po_number,
                                    posuffix = d.Key.po_suffix,
                                    Total = d.Count()

                                }).OrderBy(x => x.Remotekey).ThenBy(x => x.posuffix).ToList();
            var groupPonew889 = (from c in mylist

                                 group c by new { abs_num = c.abs_num.ToUpper() } into d

                                 select new
                                 {
                                     absnum = d.Key.abs_num,
                                     Total = d.Count()

                                 }).ToList();
            var groupPonew2 = (from c in mylist

                               group c by new { c.po_number, c.po_suffix } into d
                               select new
                               {
                                   Remotekey = d.Key.po_number,
                                   posuffix = d.Key.po_suffix,
                                   Total = d.Sum(x => x.quantity)

                               }).OrderBy(x => x.Remotekey).ThenBy(x => x.posuffix).ToList();
            int calzala = 0; string wponumber = "", wposuffix = "", empnum = "";
            var takeoff = db1.AbsnumRemoves.ToList();
            foreach (var item999 in takeoff)
            {
                mylistinvoice.RemoveAll(item7 => item7.abs_num.ToUpper() == item999.AbsnumName.ToUpper());
            }

            mylistfinal = mylist;
            int addwpik = 0;
            decimal? itemqty = 0;
            decimal? newquantity2 = 0; mylistfinal5.Clear();
            foreach (var item1 in wsetting)
            {
                var wamm = db1.SettingRates.FirstOrDefault(x => x.SettingId == item1.SettingId);
                if (wamm != null)
                {
                    item1.RateAmount = wamm.RateAmount;
                }
                else
                {
                    item1.RateAmount = 0;
                }
                item1.abs_num = "";
            }
            var testpallet = 0;
            var foundsettingcustomer = db1.SettingCustomers.FirstOrDefault(x => x.SettingBaseConum == conum && x.SettingBaseWhnum == whnum);
            var hincrem = 0; int hourlymin = 0;
            foreach (var item1 in wsetting)
            {

                var forflat = 0;
                db1.Dispose(); db1 = new RateContext();
                var wamm = db1.SettingRates.FirstOrDefault(x => x.SettingId == item1.SettingId);
                if (wamm != null)
                {
                    item1.RateAmount = wamm.RateAmount;
                }else
                {
                    item1.RateAmount = 0;
                }
                item1.abs_num = "";
                var foundRatecal = db1.Ratebys.Where(x => x.RatebyId == item1.RatebyId).FirstOrDefault();
                var foundNameabrg = db1.RateDescs.Where(x => x.RateDescId == item1.RateDescId).FirstOrDefault();
                if (foundRatecal != null && foundNameabrg != null)
                {
                    if (foundRatecal.CalculRate != 31)
                    {
                        
                        
                         if (foundRatecal.CalculRate == 200)
                        {

                            foreach (var mygroup in groupPonew88)
                            {
                                AudiViewModel red = new AudiViewModel();
                                red.substat_code = item1.RateDescId;
                                red.wh_num = whnum;
                                red.co_num = conum;
                                red.oldunitcost = item1.SettingMin;
                                red.transmission = item1.SettingId;
                                red.action_code = item1.UnitCode;
                                red.unitcost = item1.RateAmount; red.quantity = 1;
                                red.fromdatetime = item1.ChargeName; red.po_number = mygroup.Remotekey; red.po_suffix = mygroup.posuffix; red.abs_num = "";
                                red.po_line = 0; red.lot = ""; red.row_status = ""; red.sugg_qty = 1;
                                red.act_qty = 0; red.item_type = ""; red.emp_num = "0";
                                red.result_msg = ""; red.case_qtyitem = 0; red.box_qtyitem = 0;
                                itemqty = 0;
                                red.extdcost = item1.RateAmount * 1;
                                red.doc_id = foundNameabrg.NameAbrg;
                                red.guid = item1.RateAmount * 1;
                                red.cc_type = item1.UnitCode;

                                if (item1.SettingMin > red.guid)
                                {


                                    red.guid = item1.SettingMin;

                                }
                                if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                {
                                    red.guid = item1.SettingMax;
                                }
                                mylistfinal.Add(red);
                            }
                        }
                        else
                        {
                            if (foundRatecal.CalculRate != 31)
                            {
                                if (foundRatecal.CalculRate != 9)
                                {

                                    if (foundRatecal.PalletId == 0)
                                    {
                                        var teststring = ""; var wssuf = ""; var lll = 0;
                                        if (foundRatecal.CalculRate == 30 && inout == 2)
                                        {
                                            foreach (var mygroup in groupPonew2)
                                            {


                                                item1.RateAmount = wamm.RateAmount;
                                                AudiViewModel red = new AudiViewModel();
                                                red.substat_code = item1.RateDescId;
                                                red.wh_num = whnum;
                                                red.co_num = conum;
                                                red.oldunitcost = item1.SettingMin;
                                                red.unitcost = item1.RateAmount;
                                                red.transmission = item1.SettingId;
                                                red.fromdatetime = item1.ChargeName; red.po_number = ""; red.po_suffix = ""; red.abs_num = "";
                                                red.po_line = 0; red.lot = ""; red.row_status = ""; red.sugg_qty = 0;
                                                red.act_qty = 0; red.item_type = ""; red.emp_num = "0";
                                                red.result_msg = ""; red.case_qtyitem = 0; red.box_qtyitem = 0;
                                                itemqty = 0; red.quantity = 0;
                                                red.extdcost = 0;
                                                red.doc_id = foundNameabrg.NameAbrg;
                                                red.guid = 0;
                                                red.cc_type = item1.UnitCode;



                                                var findco = (from c in db1.PoCloseRequireds
                                                              where c.Poconum == conum && c.Powhnum == whnum
                                                              && c.Poponumber == mygroup.Remotekey && c.Posuffix == mygroup.posuffix
                                                              && c.PoCloseDescId == item1.RateDescId
                                                              select c).FirstOrDefault();
                                                try
                                                {
                                                    if (findco.PoCloseRate != 0)
                                                    {
                                                        if (item1.UnitCode.ToUpper() == "HR" || item1.UnitCode.ToUpper() == "HHR")
                                                        {
                                                            if (foundsettingcustomer != null)
                                                            {
                                                                if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                findco.PoCloseRate = Getquantity("HR", hincrem, hourlymin, (decimal)findco.PoCloseRate);
                                                            }
                                                        }
                                                    }
                                                    red.guid = item1.RateAmount * findco.PoCloseRate;
                                                    red.extdcost = item1.RateAmount * findco.PoCloseRate;
                                                    red.quantity = findco.PoCloseRate;
                                                    red.sugg_qty = findco.PoCloseRate;
                                                    red.po_number = mygroup.Remotekey;
                                                    red.po_suffix = mygroup.posuffix;
                                                }
                                                catch (Exception)
                                                {

                                                    red.guid = 0;
                                                    red.extdcost = 0;
                                                    red.quantity = 0;
                                                    red.sugg_qty = 0;
                                                    red.po_number = "delete";
                                                }
                                                if (item1.SettingMin > red.guid && red.guid != 0)
                                                {


                                                    red.guid = item1.SettingMin;

                                                }
                                                if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                                {
                                                    red.guid = item1.SettingMax;
                                                }
                                                if (red.quantity != 0) 
                                                    mylistfinal.Add(red);
                                                //}
                                            }
                                        }
                                        else
                                        {
                                            var kkk = 0; var powpo = ""; var suwsuf = "";
                                            if (foundRatecal.CalculRate == 200)
                                            {

                                                foreach (var mygroup in groupPonew88)
                                                {
                                                    AudiViewModel red = new AudiViewModel();
                                                    red.substat_code = item1.RateDescId;
                                                    red.wh_num = whnum;
                                                    red.co_num = conum;
                                                    red.oldunitcost = item1.SettingMin;
                                                    red.transmission = item1.SettingId;
                                                    red.action_code = item1.UnitCode;
                                                    red.unitcost = item1.RateAmount; red.quantity = 1;
                                                    red.fromdatetime = item1.ChargeName; red.po_number = mygroup.Remotekey; red.po_suffix = mygroup.posuffix; red.abs_num = "";
                                                    red.po_line = 0; red.lot = ""; red.row_status = ""; red.sugg_qty = 1;
                                                    red.act_qty = 0; red.item_type = ""; red.emp_num = "0";
                                                    red.result_msg = ""; red.case_qtyitem = 0; red.box_qtyitem = 0;
                                                    itemqty = 0;
                                                    red.extdcost = item1.RateAmount * 1;
                                                    red.doc_id = foundNameabrg.NameAbrg;
                                                    red.guid = item1.RateAmount * 1;
                                                    red.cc_type = item1.UnitCode;

                                                    if (item1.SettingMin > red.guid)
                                                    {


                                                        red.guid = item1.SettingMin;

                                                    }
                                                    if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                                    {
                                                        red.guid = item1.SettingMax;
                                                    }
                                                    mylistfinal.Add(red);
                                                }
                                            }
                                            
                                            if (foundRatecal.CalculRate == 320) 
                                            {
                                                testpallet = foundNameabrg.TypeCalculate;

                                            }
                                            if (foundRatecal.CalculRate == 2666)
                                            {
                                                var gropp = groupPonew78.Where(x => x.Total != 0).OrderBy(x => x.Remotekey).ToList();
                                                var bo = 0;var myponumber = "";
                                                foreach (var mygroup in gropp)
                                                {
                                                    
                                                        var newqtyototal = 0.00m;
                                                        var wempnum = ""; var wllot = "";
                                                    if (myponumber != mygroup.Remotekey)
                                                    {

                                                        myponumber = mygroup.Remotekey;
                                                        var pickTable222 = (from c in db.picks
                                                                          join k in db.items on new { c.co_num, c.wh_num, abs_num = c.abs_num.ToUpper() } equals new { k.co_num, k.wh_num, abs_num = k.abs_num.ToUpper() }
                                                                          where c.order_ == mygroup.Remotekey && c.order_suffix == mygroup.posuffix
                                                                         

                                                                          select new
                                                                          {
                                                                              Remotekey = c.order_,
                                                                              Rabsnum = c.abs_num,
                                                                              posuffix = c.order_suffix,
                                                                              binnum = c.bin_num,
                                                                              empnum = k.prod_grp,
                                                                              lot = c.lot,
                                                                              Total = c.qty
                                                                          }).OrderBy(x => x.Remotekey).ToList();
                                                        

                                                        foreach (var pictabl in pickTable222)
                                                        {
                                                            try
                                                            {
                                                                wllot = pictabl.lot;
                                                                var witemm = db.items.FirstOrDefault(x => x.abs_num.ToUpper() == pictabl.Rabsnum.ToUpper() && x.co_num == conum && x.wh_num == whnum);
                                                                wempnum = pictabl.empnum; newqtyototal = Convert.ToDecimal(pictabl.Total);
                                                                var nnewq = pictabl.Total - (((int)(pictabl.Total / witemm.pallet_qty)) * witemm.pallet_qty);

                                                                var newtotalnew = pictabl.Total;
                                                                if (pictabl.Total >= witemm.pallet_qty)
                                                                {
                                                                    nnewq = pictabl.Total - (((int)(pictabl.Total / witemm.pallet_qty)) * witemm.pallet_qty);
                                                                    newtotalnew = nnewq;
                                                                }
                                                                if (newtotalnew < witemm.case_qty)
                                                                {
                                                                    nnewq = 0;
                                                                }
                                                                else
                                                                {
                                                                    if (newtotalnew <= witemm.case_qty)
                                                                    {
                                                                        nnewq = newtotalnew;
                                                                    }
                                                                    else
                                                                    {
                                                                        nnewq = (int)(newtotalnew / witemm.case_qty);
                                                                    }
                                                                }
                                                                if (witemm.uom.ToUpper() == "EA")
                                                                {
                                                                    if (pictabl.Total < witemm.pallet_qty)
                                                                    {
                                                                        nnewq = (int)(pictabl.Total / witemm.case_qty);
                                                                    }
                                                                    else
                                                                    {

                                                                        nnewq = pictabl.Total - (((int)(pictabl.Total / witemm.pallet_qty)) * witemm.pallet_qty);
                                                                        if (nnewq < witemm.case_qty)
                                                                        {

                                                                        }
                                                                        else
                                                                        {
                                                                            nnewq = nnewq - (((int)(nnewq / witemm.case_qty)) * witemm.case_qty);
                                                                        }

                                                                    }
                                                                }




                                                                if (nnewq != 0)
                                                                {
                                                                    var red = new AudiViewModel();
                                                                    try
                                                                    {
                                                                        var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item1.RateDescId);
                                                                        if (df.ForGroup == 1)
                                                                        {
                                                                            var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item1.RateDescId && x.GroupName.Trim() == wempnum.Trim() && x.KindId == inout).FirstOrDefault();
                                                                            item1.RateAmount = foundrateforGroup.GroupRate;
                                                                        }
                                                                    }
                                                                    catch (Exception)
                                                                    {
                                                                        if (wempnum.Trim().Length != 0) { item1.RateAmount = 0; item1.SettingMin = 0; }

                                                                    }
                                                                    red.extdcost = 0; red.quantity = nnewq;
                                                                    red.substat_code = item1.RateDescId;
                                                                    red.wh_num = whnum;
                                                                    red.co_num = conum;
                                                                    red.emp_num = wempnum;
                                                                    red.oldunitcost = item1.SettingMin;
                                                                    red.transmission = item1.SettingId;
                                                                    red.unitcost = item1.RateAmount;
                                                                    red.extdcost = item1.RateAmount * red.quantity;
                                                                    red.doc_id = foundNameabrg.NameAbrg;
                                                                    red.lot = pictabl.lot;
                                                                    red.abs_num = pictabl.Rabsnum;

                                                                    red.substat_code = item1.RateDescId;
                                                                    red.extdcost = item1.RateAmount;
                                                                    red.action_code = item1.UnitCode;
                                                                    red.transmission = item1.SettingId;
                                                                    red.wh_num = whnum;
                                                                    red.co_num = conum;
                                                                    red.oldunitcost = item1.SettingMin;
                                                                    red.unitcost = item1.RateAmount;
                                                                    red.fromdatetime = item1.ChargeName;
                                                                    red.po_number = pictabl.Remotekey;
                                                                    red.po_suffix = pictabl.posuffix;
                                                                    red.quantity = nnewq;
                                                                    red.sugg_qty = pictabl.Total;
                                                                    if (item1.UnitCode.ToUpper() == "HR" || item1.UnitCode.ToUpper() == "HHR")
                                                                    {
                                                                        if (foundsettingcustomer != null)
                                                                        {
                                                                            if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                            if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                            if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                            hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                            red.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red.quantity);
                                                                        }
                                                                    }

                                                                    red.guid = item1.RateAmount * red.quantity;
                                                                    red.doc_id = foundNameabrg.NameAbrg;
                                                                    if (item1.SettingMax > 0)
                                                                    {
                                                                        if (red.guid > item1.SettingMax)
                                                                        {
                                                                            red.guid = item1.SettingMax;
                                                                        }
                                                                    }
                                                                    if (item1.SettingMin > 0)
                                                                    {

                                                                        if (red.guid < item1.SettingMin)
                                                                        {
                                                                            red.guid = item1.SettingMin;
                                                                        }
                                                                    }
                                                                    red.case_qtyitem = 0;
                                                                    red.box_qtyitem = 0;
                                                                    if (red.po_number == "201907-7663" && red.po_suffix == "02" && red.abs_num == "10890817001003" && red.lot == "06042019")
                                                                    {
                                                                        var redebb = mylistfinal.Where(x => x.po_number == "201907-7663" && x.po_suffix == "02" && x.abs_num == "10890817001003" && x.lot == "06042019").ToList();

                                                                    }
                                                                    mylistfinal.Add(red);

                                                                }
                                                            }
                                                            catch (Exception)
                                                            {

                                                                
                                                            }
                                                       
                                                            // newqtyo += Convert.ToDecimal(nnewq);
                                                        }
                                                        if (bo == 0)
                                                        {
                                                            bo = 1;
                                                            myponumber = mygroup.Remotekey;

                                                        }
                                                        myponumber = mygroup.Remotekey;
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                var grouppn = groupPonew.Where(x => x.Total != 0).ToList();
                                                foreach (var mygroup in grouppn)
                                                {
                                                    kkk += 1;

                                                    item1.RateAmount = wamm.RateAmount;

                                                    
           
                                                    if (foundRatecal.CalculRate == 2666)
                                                    {
                                                        var pickTable2 = (from c in db.picks
                                                                          join k in db.items on new { c.co_num, c.wh_num, abs_num = c.abs_num.ToUpper() } equals new { k.co_num, k.wh_num, abs_num = k.abs_num.ToUpper() }
                                                                          where c.order_ == mygroup.Remotekey && c.order_suffix == mygroup.posuffix && c.abs_num.ToUpper() == mygroup.Rabsnum.ToUpper()

                                                                          select new
                                                                          {
                                                                              Remotekey = c.order_,
                                                                              Rabsnum = c.abs_num,
                                                                              posuffix = c.order_suffix,
                                                                              binnum = c.bin_num,
                                                                              empnum = k.prod_grp,
                                                                              lot = c.lot,
                                                                              Total = c.qty
                                                                          }).OrderBy(x => x.Remotekey).ToList();
                                                        var totalProdcuts = from o in pickTable2
                                                                            group o by new { o.empnum, o.lot, o.posuffix, o.Rabsnum, o.Remotekey, o.binnum } into g
                                                                            select new
                                                                            {
                                                                                empnum = g.Key.empnum,
                                                                                lot = g.Key.lot,
                                                                                posuffix = g.Key.posuffix,
                                                                                binum = g.Key.binnum,
                                                                                Rabsnum = g.Key.Rabsnum,
                                                                                Remotekey = g.Key.Remotekey,
                                                                                Total = g.Sum(x => x.Total)
                                                                            };
                                                        var newqtyototal = 0.00m;
                                                        var wempnum = ""; var wllot = "";

                                                        foreach (var pictabl in totalProdcuts)
                                                        {
                                                            wllot = pictabl.lot;
                                                            var witemm = db.items.FirstOrDefault(x => x.abs_num.ToUpper() == pictabl.Rabsnum.ToUpper() && x.co_num == conum && x.wh_num == whnum);
                                                            wempnum = pictabl.empnum; newqtyototal = Convert.ToDecimal(pictabl.Total);
                                                            var nnewq = pictabl.Total - (((int)(pictabl.Total / witemm.pallet_qty)) * witemm.pallet_qty);

                                                            var newtotalnew = pictabl.Total;
                                                            if (pictabl.Total >= witemm.pallet_qty)
                                                            {
                                                                nnewq = pictabl.Total - (((int)(pictabl.Total / witemm.pallet_qty)) * witemm.pallet_qty);
                                                                newtotalnew = nnewq;
                                                            }
                                                            if (newtotalnew < witemm.case_qty)
                                                            {
                                                                nnewq = 0;
                                                            }
                                                            else
                                                            {
                                                                nnewq = (int)(newtotalnew / witemm.case_qty);
                                                            }
                                                            if (witemm.uom.ToUpper() == "EA")
                                                            {
                                                                if (pictabl.Total < witemm.pallet_qty)
                                                                {
                                                                    nnewq = (int)(pictabl.Total / witemm.case_qty);
                                                                }
                                                                else
                                                                {

                                                                    nnewq = pictabl.Total - (((int)(pictabl.Total / witemm.pallet_qty)) * witemm.pallet_qty);
                                                                    if (nnewq < witemm.case_qty)
                                                                    {

                                                                    }
                                                                    else
                                                                    {
                                                                        nnewq = nnewq - (((int)(nnewq / witemm.case_qty)) * witemm.case_qty);
                                                                    }

                                                                }
                                                            }




                                                            if (nnewq != 0)
                                                            {
                                                                var red = new AudiViewModel();
                                                                try
                                                                {
                                                                    var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item1.RateDescId);
                                                                    if (df.ForGroup == 1)
                                                                    {
                                                                        var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item1.RateDescId && x.GroupName.Trim() == wempnum.Trim() && x.KindId == inout).FirstOrDefault();
                                                                        item1.RateAmount = foundrateforGroup.GroupRate;
                                                                    }
                                                                }
                                                                catch (Exception)
                                                                {
                                                                    if (wempnum.Trim().Length != 0) { item1.RateAmount = 0; item1.SettingMin = 0; }

                                                                }
                                                                red.extdcost = 0; red.quantity = nnewq;
                                                                red.substat_code = item1.RateDescId;
                                                                red.wh_num = whnum;
                                                                red.co_num = conum;
                                                                red.emp_num = wempnum;
                                                                red.oldunitcost = item1.SettingMin;
                                                                red.transmission = item1.SettingId;
                                                                red.unitcost = item1.RateAmount;
                                                                red.extdcost = item1.RateAmount * red.quantity;
                                                                red.doc_id = foundNameabrg.NameAbrg;
                                                                red.lot = pictabl.lot;
                                                                red.abs_num = pictabl.Rabsnum;

                                                                red.substat_code = item1.RateDescId;
                                                                red.extdcost = item1.RateAmount;
                                                                red.action_code = item1.UnitCode;
                                                                red.transmission = item1.SettingId;
                                                                red.wh_num = whnum;
                                                                red.co_num = conum;
                                                                red.oldunitcost = item1.SettingMin;
                                                                red.unitcost = item1.RateAmount;
                                                                red.fromdatetime = item1.ChargeName;
                                                                red.po_number = pictabl.Remotekey;
                                                                red.po_suffix = pictabl.posuffix;
                                                                red.quantity = nnewq;
                                                                red.sugg_qty = pictabl.Total;
                                                                if (item1.UnitCode.ToUpper() == "HR" || item1.UnitCode.ToUpper() == "HHR")
                                                                {
                                                                    if (foundsettingcustomer != null)
                                                                    {
                                                                        if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                        if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                        if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                        hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                        red.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red.quantity);
                                                                    }
                                                                }

                                                                red.guid = item1.RateAmount * red.quantity;
                                                                red.doc_id = foundNameabrg.NameAbrg;
                                                                if (item1.SettingMax > 0)
                                                                {
                                                                    if (red.guid > item1.SettingMax)
                                                                    {
                                                                        red.guid = item1.SettingMax;
                                                                    }
                                                                }
                                                                if (item1.SettingMin > 0)
                                                                {

                                                                    if (red.guid < item1.SettingMin)
                                                                    {
                                                                        red.guid = item1.SettingMin;
                                                                    }
                                                                }
                                                                red.case_qtyitem = 0;
                                                                red.box_qtyitem = 0;
                                                                mylistfinal.Add(red);
                                                            }
                                                            
                                                        }

                                                    }
             
                                                    else if (inout == 2 && foundRatecal.CalculRate == 20)
                                                    {
                                                        if (teststring != mygroup.Remotekey || wssuf != mygroup.posuffix)
                                                        {
                                                            int countfinal = 0;
                                                            var calresult = (from c in db.auditlogs
                                                                             where c.po_number == mygroup.Remotekey && c.po_suffix == mygroup.posuffix && c.wh_num == whnum
                                                                             && c.co_num == conum && String.IsNullOrEmpty(c.lot) != true && c.trans_type.ToUpper() == "IG"
                                                                             select c).ToList();
                                                            countfinal = calresult.Count;
                                                            AudiViewModel red = new AudiViewModel();
                                                            red.substat_code = item1.RateDescId;
                                                            red.wh_num = whnum;
                                                            red.co_num = conum;
                                                            //  red.lot = mygroup.wlot;
                                                            red.oldunitcost = item1.SettingMin;
                                                            red.unitcost = item1.RateAmount;
                                                            red.transmission = item1.SettingId;
                                                            red.action_code = item1.UnitCode;
                                                            red.fromdatetime = item1.ChargeName; red.po_number = mygroup.Remotekey; red.po_suffix = mygroup.posuffix; red.abs_num = "";
                                                            red.po_line = 0; red.lot = ""; red.row_status = ""; red.sugg_qty = countfinal;
                                                            red.act_qty = 0; red.item_type = ""; red.emp_num = "0";
                                                            red.result_msg = ""; red.case_qtyitem = 0; red.box_qtyitem = 0;
                                                            itemqty = 0; red.quantity = countfinal;
                                                            red.extdcost = item1.RateAmount * countfinal;
                                                            red.doc_id = foundNameabrg.NameAbrg;
                                                            red.guid = item1.RateAmount * countfinal;
                                                            red.cc_type = item1.UnitCode;

                                                            if (item1.SettingMin > red.guid)
                                                            {


                                                                red.guid = item1.SettingMin;

                                                            }
                                                            if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                                            {
                                                                red.guid = item1.SettingMax;
                                                            }
                                                            mylistfinal.Add(red);
                                                            
                                                        }
                                                        
                                                        teststring = mygroup.Remotekey;
                                                        wssuf = mygroup.posuffix;
                                                    }
                                                    else if (inout == 2 && foundRatecal.CalculRate == 71)
                                                    {

                                                        if (teststring != mygroup.Remotekey || wssuf != mygroup.posuffix)
                                                        {
                                                           
                                                            var calresult = (from c in db.auditlogs
                                                                             where c.po_number == mygroup.Remotekey && c.po_suffix == mygroup.posuffix && c.wh_num == whnum
                                                                             && c.co_num == conum && c.lot != "" && c.lot != "NULL"
                                                                             select c).GroupBy(x => new { x.lot, abs_num = x.abs_num.ToUpper() }).ToList();
                                                            var wi = 0;
                                                            decimal? amountofcal = 0;
                                                            foreach (var itemcal in calresult)
                                                            {
                                                                wi += 1;
                                                                AudiViewModel red = new AudiViewModel();
                                                                red.substat_code = item1.RateDescId;
                                                                red.wh_num = whnum;
                                                                red.co_num = conum; red.abs_num = itemcal.Key.abs_num;
                                                                red.transmission = item1.SettingId;
                                                                red.oldunitcost = item1.SettingMin;
                                                                red.unitcost = item1.RateAmount;
                                                                red.fromdatetime = item1.ChargeName; red.po_number = mygroup.Remotekey; red.po_suffix = mygroup.posuffix;
                                                                red.po_line = 0; red.lot = itemcal.Key.lot; red.row_status = ""; red.sugg_qty = 1;
                                                                red.act_qty = 0; red.item_type = ""; red.emp_num = "0"; red.quantity = 1;
                                                                red.result_msg = red.po_number + " " + red.po_suffix; red.case_qtyitem = 0; red.box_qtyitem = 0;
                                                                itemqty = 0; red.cc_type = item1.UnitCode;
                                                                red.extdcost = item1.RateAmount * 1;
                                                                //  red.lot = mygroup.wlot;
                                                                red.doc_id = foundNameabrg.NameAbrg;

                                                                if (red.oldunitcost > red.extdcost)
                                                                {
                                                                    if (calresult.Count == 1)
                                                                        red.guid = item1.SettingMin;
                                                                }


                                                                else
                                                                {

                                                                    red.guid = item1.RateAmount * 1;


                                                                }
                                                                if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                                                {
                                                                    if (calresult.Count == 1)
                                                                        red.guid = item1.SettingMax;
                                                                }
                                                                if (calresult.Count > 1)
                                                                {
                                                                    amountofcal += item1.RateAmount * 1;
                                                                    red.guid = item1.RateAmount * 1;
                                                                }
                                                                mylistfinal.Add(red);
                                                            }
                                                            if (calresult.Count > 1)
                                                            {
                                                                if (amountofcal > item1.SettingMax && item1.SettingMax != 0)
                                                                {
                                                                    AudiViewModel red = new AudiViewModel();
                                                                    red.substat_code = item1.RateDescId;
                                                                    red.wh_num = whnum;
                                                                    red.co_num = conum; red.abs_num = "";
                                                                    red.transmission = item1.SettingId;
                                                                    red.oldunitcost = item1.SettingMin;
                                                                    red.unitcost = item1.RateAmount;
                                                                    red.fromdatetime = item1.ChargeName; red.po_number = mygroup.Remotekey; red.po_suffix = mygroup.posuffix; red.abs_num = "";
                                                                    red.po_line = 0; red.lot = ""; red.row_status = ""; red.sugg_qty = 1;
                                                                    red.act_qty = 0; red.item_type = ""; red.emp_num = "0"; red.quantity = 1;
                                                                    red.result_msg = red.po_number + " " + red.po_suffix; red.case_qtyitem = 0; red.box_qtyitem = 0;
                                                                    itemqty = 0; red.cc_type = item1.UnitCode;
                                                                    red.extdcost = amountofcal - item1.SettingMax;
                                                                    //  red.lot = mygroup.wlot;
                                                                    red.doc_id = foundNameabrg.NameAbrg; red.guid = amountofcal - item1.SettingMax;
                                                                    mylistfinal.Add(red);
                                                                }
                                                                if (amountofcal < item1.SettingMin && item1.SettingMin != 0)
                                                                {
                                                                    AudiViewModel red = new AudiViewModel();
                                                                    red.substat_code = item1.RateDescId;
                                                                    red.wh_num = whnum;
                                                                    red.co_num = conum; red.abs_num = "";
                                                                    red.transmission = item1.SettingId;
                                                                    red.oldunitcost = item1.SettingMin;
                                                                    red.unitcost = item1.RateAmount;
                                                                    red.fromdatetime = item1.ChargeName; red.po_number = mygroup.Remotekey; red.po_suffix = mygroup.posuffix; red.abs_num = "";
                                                                    red.po_line = 0; red.lot = ""; red.row_status = ""; red.sugg_qty = 1;
                                                                    red.act_qty = 0; red.item_type = ""; red.emp_num = "0"; red.quantity = 1;
                                                                    red.result_msg = red.po_number + " " + red.po_suffix; red.case_qtyitem = 0; red.box_qtyitem = 0;
                                                                    itemqty = 0; red.cc_type = item1.UnitCode;
                                                                    red.extdcost = item1.SettingMin - amountofcal; red.oldunitcost = item1.SettingMin - amountofcal;
                                                                    red.doc_id = foundNameabrg.NameAbrg; red.guid = item1.SettingMin - amountofcal;
                                                                    mylistfinal.Add(red);
                                                                }
                                                            }



                                                        }
                                                        teststring = mygroup.Remotekey;
                                                        wssuf = mygroup.posuffix;



                                                    }
                                                    
                                                    else if (inout == 2 && foundRatecal.CalculRate == 8)
                                                    {

                                                        if (teststring != mygroup.Remotekey || wssuf != mygroup.posuffix)
                                                        {

                                                            var calresult = (from c in db.orddtls
                                                                             join k in db.ordhdrs on c.id equals k.id
                                                                             where k.order_ == mygroup.Remotekey && k.order_suffix == mygroup.posuffix && k.wh_num == whnum
                                                                             && k.co_num == conum
                                                                             select c).ToList();

                                                            var resa = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item1.RateDescId);
                                                            if (resa.TypeCalculate == 4)
                                                            {
                                                                calresult = (from c in db.orddtls
                                                                             join k in db.ordhdrs on c.id equals k.id
                                                                             join p in db.items on new { k.co_num, k.wh_num, c.abs_num } equals new { p.co_num, p.wh_num, p.abs_num }
                                                                             where k.order_ == mygroup.Remotekey && k.order_suffix == mygroup.posuffix && k.wh_num == whnum
                                                                             && k.co_num == conum && p.lot_ctrl == 1
                                                                             select c).ToList();

                                                            }
                                                            AudiViewModel red = new AudiViewModel();
                                                            red.substat_code = item1.RateDescId;
                                                            red.wh_num = whnum;
                                                            red.co_num = conum;
                                                            red.transmission = item1.SettingId;
                                                            red.oldunitcost = item1.SettingMin;
                                                            red.unitcost = item1.RateAmount;
                                                            red.fromdatetime = item1.ChargeName; red.po_number = mygroup.Remotekey; red.po_suffix = mygroup.posuffix; red.abs_num = "";
                                                            red.po_line = 0; red.lot = ""; red.row_status = ""; red.sugg_qty = calresult.Count;
                                                            red.act_qty = 0; red.item_type = ""; red.emp_num = "0";
                                                            red.result_msg = red.po_number + " " + red.po_suffix; red.case_qtyitem = 0; red.box_qtyitem = 0;
                                                            itemqty = 0; red.cc_type = item1.UnitCode;
                                                            // red.lot = mygroup.wlot;
                                                            red.extdcost = item1.RateAmount * calresult.Count; red.quantity = calresult.Count;
                                                            red.doc_id = foundNameabrg.NameAbrg;

                                                            if (red.oldunitcost > red.extdcost)
                                                            {
                                                                red.guid = item1.SettingMin;
                                                            }


                                                            else
                                                            {

                                                                red.guid = item1.RateAmount * calresult.Count;


                                                            }
                                                            if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                                            {
                                                                red.guid = item1.SettingMax;
                                                            }
                                                            mylistfinal.Add(red);





                                                        }
                                                        teststring = mygroup.Remotekey;
                                                        wssuf = mygroup.posuffix;


                                                    }
                                                    
                        
                                                    else if (foundRatecal.CalculRate == 320)
                                                    {
                                                        var thegroup = db.items.FirstOrDefault(x => x.co_num == conum && x.wh_num == whnum && x.abs_num.Trim().ToUpper() == mygroup.Rabsnum.Trim().ToUpper());

                                                        try
                                                        {
                                                            var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item1.RateDescId);
                                                            if (df.ForGroup == 1)
                                                            {
                                                                var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item1.RateDescId && x.GroupName.Trim() == thegroup.prod_grp.Trim() && x.KindId == inout).FirstOrDefault();
                                                                item1.RateAmount = foundrateforGroup.GroupRate;
                                                            }

                                                        }
                                                        catch (Exception)
                                                        {
                                                            if (thegroup.prod_grp.Length != 0) { item1.RateAmount = 0; item1.SettingMin = 0; }

                                                        }
                                                        var wcalcul = 0;
                                                        if (testpallet == 1) wcalcul = 260; if (testpallet == 2) wcalcul = 2610; if (testpallet == 3) wcalcul = 26100;

                                                        

                                                        var byloca = db.picks.Where(x => x.order_ == mygroup.Remotekey && x.order_suffix == mygroup.posuffix && x.co_num == conum && x.wh_num == whnum && x.abs_num.Trim().ToUpper() == mygroup.Rabsnum.Trim().ToUpper()).ToList();
                                                        var groupPonewbyloca = (from c in byloca

                                                                                group c by new { c.bin_num } into d

                                                                                select new
                                                                                {
                                                                                    binnum = d.Key.bin_num,
                                                                                    Total = d.Sum(x => x.qty)

                                                                                }).OrderBy(x => x.binnum).ToList();
                                                        foreach (var wbyloca in groupPonewbyloca)
                                                        {
                                                            Tuple<decimal?, decimal?, decimal?> myswitch = CalculHandling.CalculRateFore(wcalcul, 0, 0, 0, item1.RateAmount, wbyloca.Total, wbyloca.Total, thegroup.pallet_qty, thegroup.box_qty, thegroup.case_qty);
                                                            AudiViewModel red = new AudiViewModel();
                                                            var mylistgo = mylist.FirstOrDefault(x => x.po_number.Trim() == mygroup.Remotekey.Trim() && x.po_suffix.Trim() == mygroup.posuffix.Trim() && x.abs_num.Trim().ToUpper() == mygroup.Rabsnum.Trim().ToUpper() && x.bin_to == "x");
                                                            if (mylistgo != null)
                                                            {
                                                                red.extdcost = myswitch.Item1; red.quantity = myswitch.Item3;
                                                                red.substat_code = item1.RateDescId;
                                                                red.wh_num = whnum;
                                                                red.co_num = conum;
                                                                red.oldunitcost = item1.SettingMin;
                                                                red.transmission = item1.SettingId;
                                                                red.unitcost = item1.RateAmount;
                                                                red.extdcost = item1.RateAmount * red.quantity;
                                                                red.doc_id = foundNameabrg.NameAbrg;
                                                                
                                                                if (red.quantity != 0)
                                                                {
                                                                    if (item1.UnitCode.ToUpper() == "HR" || item1.UnitCode.ToUpper() == "HHR")
                                                                    {
                                                                        if (foundsettingcustomer != null)
                                                                        {
                                                                            if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                            if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                            if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                            hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                            red.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red.quantity);
                                                                        }
                                                                    }
                                                                }
                                                                red.guid = item1.RateAmount * red.quantity;
                                                                red.fromdatetime = item1.ChargeName; red.po_number = mylistgo.po_number; red.po_suffix = mylistgo.po_suffix; red.abs_num = mylistgo.abs_num;
                                                                red.po_line = mylistgo.po_line; red.lot = mylistgo.lot; red.row_status = mylistgo.row_status; red.sugg_qty = wbyloca.Total;
                                                                red.act_qty = mylistgo.act_qty; red.item_type = mylistgo.item_type; red.date_time2 = mylistgo.date_time2; red.emp_num = mylistgo.emp_num;
                                                                red.result_msg = mylistgo.result_msg; red.case_qtyitem = mylistgo.case_qtyitem; red.box_qtyitem = mylistgo.box_qtyitem;
                                                                itemqty = mylistgo.item_qty; red.cc_type = mylistgo.cc_type; red.action_code = item1.UnitCode;
                                                                if (item1.SettingMin > red.guid)
                                                                {
                                                                    red.guid = item1.SettingMin;
                                                                }
                                                                if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                                                {
                                                                    red.guid = item1.SettingMax;
                                                                }
                                                                mylistfinal.Add(red);
                                                            }
                                                        }
                                                        
                                                    }
                                                    else if (foundRatecal.CalculRate == 260)
                                                    {
                                                        var pickTable2 = (from c in db.picks
                                                                          join k in db.items on new { c.co_num, c.wh_num, abs_num = c.abs_num.ToUpper() } equals new { k.co_num, k.wh_num, abs_num = k.abs_num.ToUpper() }
                                                                          where c.order_ == mygroup.Remotekey && c.order_suffix == mygroup.posuffix && c.abs_num.ToUpper() == mygroup.Rabsnum.ToUpper()

                                                                          select new
                                                                          {
                                                                              Remotekey = c.order_,
                                                                              Rabsnum = c.abs_num,
                                                                              posuffix = c.order_suffix,
                                                                              empnum = k.prod_grp,
                                                                              lot = c.lot,
                                                                              pallet = k.pallet_qty,
                                                                              binnum = c.bin_num,
                                                                              Total = c.qty
                                                                          }).OrderBy(x => x.Remotekey).ToList();
                                                        var totalProdcuts = from o in pickTable2
                                                                            group o by new { o.empnum, o.lot, o.posuffix, o.Rabsnum, o.Remotekey, o.binnum, o.pallet } into g
                                                                            select new
                                                                            {
                                                                                empnum = g.Key.empnum,
                                                                                lot = g.Key.lot,
                                                                                pallet = g.Key.pallet,
                                                                                binnum = g.Key.binnum,
                                                                                posuffix = g.Key.posuffix,
                                                                                Rabsnum = g.Key.Rabsnum,
                                                                                Remotekey = g.Key.Remotekey,
                                                                                Total = g.Sum(x => x.Total)
                                                                            };
                                                        var newqtyototal = 0.00m;
                                                        var wempnum = ""; var wllot = "";

                                                        foreach (var pictabl in totalProdcuts)
                                                        {


                                                            var red = new AudiViewModel();
                                                            try
                                                            {
                                                                var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item1.RateDescId);
                                                                if (df.ForGroup == 1)
                                                                {
                                                                    var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item1.RateDescId && x.GroupName.Trim() == wempnum.Trim() && x.KindId == inout).FirstOrDefault();
                                                                    item1.RateAmount = foundrateforGroup.GroupRate;
                                                                }
                                                            }
                                                            catch (Exception)
                                                            {
                                                                if (wempnum.Trim().Length != 0) { item1.RateAmount = 0; item1.SettingMin = 0; }

                                                            }
                                                            red.extdcost = 0; red.quantity = (int)(pictabl.Total / pictabl.pallet);
                                                            red.substat_code = item1.RateDescId;
                                                            red.wh_num = whnum;
                                                            red.co_num = conum;
                                                            red.emp_num = wempnum;
                                                            red.oldunitcost = item1.SettingMin;
                                                            red.transmission = item1.SettingId;
                                                            red.unitcost = item1.RateAmount;
                                                            red.extdcost = item1.RateAmount * red.quantity;
                                                            red.doc_id = foundNameabrg.NameAbrg;
                                                            red.lot = pictabl.lot;
                                                            red.abs_num = pictabl.Rabsnum;

                                                            red.substat_code = item1.RateDescId;
                                                            red.extdcost = item1.RateAmount;
                                                            red.action_code = item1.UnitCode;
                                                            red.transmission = item1.SettingId;
                                                            red.wh_num = whnum;
                                                            red.co_num = conum;
                                                            red.oldunitcost = item1.SettingMin;
                                                            red.unitcost = item1.RateAmount;
                                                            red.fromdatetime = item1.ChargeName;
                                                            red.po_number = pictabl.Remotekey;
                                                            red.po_suffix = pictabl.posuffix;
                                                            red.quantity = (int)(pictabl.Total / pictabl.pallet);
                                                            red.sugg_qty = pictabl.Total;
                                                            if (item1.UnitCode.ToUpper() == "HR" || item1.UnitCode.ToUpper() == "HHR")
                                                            {
                                                                if (foundsettingcustomer != null)
                                                                {
                                                                    if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                    if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                    if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                    hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                    red.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red.quantity);
                                                                }
                                                            }

                                                            red.guid = item1.RateAmount * red.quantity;
                                                            red.doc_id = foundNameabrg.NameAbrg;
                                                            if (item1.SettingMax > 0)
                                                            {
                                                                if (red.guid > item1.SettingMax)
                                                                {
                                                                    red.guid = item1.SettingMax;
                                                                }
                                                            }
                                                            if (item1.SettingMin > 0)
                                                            {

                                                                if (red.guid < item1.SettingMin)
                                                                {
                                                                    red.guid = item1.SettingMin;
                                                                }
                                                            }
                                                            red.case_qtyitem = 0;
                                                            red.box_qtyitem = 0;
                                                            mylistfinal.Add(red);
                                                        }
                                                        


                                                    }
                                                    else
                                                    {

                                                        
                                                        decimal? newquatity = mygroup.Total;
                                                        var mylistgo = mylist.Where(x => x.po_number.Trim() == mygroup.Remotekey.Trim() && x.po_suffix.Trim() == mygroup.posuffix.Trim() && x.abs_num.Trim().ToUpper() == mygroup.Rabsnum.Trim().ToUpper() && x.bin_to == "x").ToList();
                                                        int zalanew = 0, forthesame = 0;
                                                        string wromote = "", wabss = "", wsuffp = "";
                                                        if (foundRatecal.CalculRate != 8)
                                                        {

                                                            if (mylistgo.Count > 1)
                                                            {
                                                                var groupPonew22 = (from c in mylistgo

                                                                                    group c by new { c.po_number, c.po_suffix, c.abs_num } into d

                                                                                    select new
                                                                                    {
                                                                                        Remotekey = d.Key.po_number,
                                                                                        Rabsnum = d.Key.abs_num,
                                                                                        posuffix = d.Key.po_suffix,
                                                                                        Total = d.Sum(x => x.quantity)

                                                                                    }).OrderBy(x => x.Remotekey).ThenBy(x => x.posuffix).FirstOrDefault();
                                                                var ytt = 0;
                                                                foreach (var item99 in mylistgo)
                                                                {
                                                                    if (ytt == 0)
                                                                    {
                                                                        ytt = 1;
                                                                        item99.quantity = groupPonew22.Total;
                                                                    }
                                                                    else
                                                                    {
                                                                        item99.quantity = 0;
                                                                    }

                                                                }
                                                            }
                                                            foreach (var item33 in mylistgo)
                                                            {
                                                                forflat += 1;
                                                                newquatity = item33.quantity;

                                                                

                                                                forthesame += 1;

                                                                try
                                                                {
                                                                    var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item1.RateDescId);
                                                                    if (df.ForGroup == 1)
                                                                    {
                                                                        var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item1.RateDescId && x.GroupName.Trim() == item33.emp_num.Trim() && x.KindId == inout).FirstOrDefault();
                                                                        item1.RateAmount = foundrateforGroup.GroupRate;
                                                                    }
                                                                }
                                                                catch (Exception)
                                                                {
                                                                    if (item33.emp_num.Trim().Length != 0) { item1.RateAmount = 0; item1.SettingMin = 0; }

                                                                }
                                                                if (item1.abs_num != item33.abs_num)
                                                                {
                                                                    zalanew += 1;
                                                                    AudiViewModel red = new AudiViewModel();
                                                                    List<InvoiceViewModel> newinvoice = mylistinvoice;
                                                                    if (foundRatecal.PalletId != 0)
                                                                    {
                                                                        if (zalanew == 1)
                                                                        {
                                                                            newquatity = CalculHandling.CalculPaletId(conum, whnum, newinvoice, mygroup.Remotekey, mygroup.posuffix, mygroup.Rabsnum, newquatity);
                                                                        }
                                                                        else
                                                                        {
                                                                            newquatity = 0; red.release_id = "N";
                                                                        }
                                                                    }
                                                                    else
                                                                    {

                                                                        string wabs = mygroup.Rabsnum;
                                                                        newquatity = CalculHandling.CalculFirst(foundRatecal.CalculRate, newinvoice, mygroup.Remotekey, mygroup.posuffix, wabs, newquatity);


                                                                    }

                                                                    red.wh_num = whnum;
                                                                    red.co_num = conum;
                                                                    red.substat_code = item1.RateDescId;
                                                                    red.oldunitcost = item1.SettingMin;
                                                                    red.unitcost = item1.RateAmount;
                                                                    red.transmission = item1.SettingId;
                                                                    if (!string.IsNullOrEmpty(item33.emp_num))
                                                                    {
                                                                        if (item33.emp_num.Trim().ToUpper() == "CONT")
                                                                        {
                                                                            red.unitcost = 0;
                                                                            red.oldunitcost = 0;
                                                                            red.quantity = 0;
                                                                        }
                                                                    }

                                                                    red.fromdatetime = item1.ChargeName; red.po_number = item33.po_number; red.po_suffix = item33.po_suffix; red.abs_num = item33.abs_num;
                                                                    red.po_line = item33.po_line; red.lot = item33.lot; red.row_status = item33.row_status; red.sugg_qty = item33.quantity;
                                                                    red.act_qty = item33.act_qty; red.item_type = item33.item_type; red.date_time2 = item33.date_time2; red.emp_num = item33.emp_num;
                                                                    red.result_msg = item33.result_msg; red.case_qtyitem = item33.case_qtyitem; red.box_qtyitem = item33.box_qtyitem;
                                                                    itemqty = item33.item_qty; red.cc_type = item33.cc_type; red.action_code = item1.UnitCode;

                                                                    decimal calc = CalculHandling.CalculSecond(foundRatecal.CalculRate, foundRatecal.PalletId, newquatity, itemqty);
                                                                    Tuple<decimal?, decimal?> myres = CalculHandling.CalculRate(foundRatecal.CalculRate, calc, newquatity, red.sugg_qty);
                                                                    calc = Convert.ToDecimal(myres.Item1);
                                                                    newquatity = myres.Item2;

                                                                    Tuple<decimal?, decimal?, decimal?> myswitch = CalculHandling.CalculRateFore(foundRatecal.CalculRate, forthesame, calc, red.extdcost, item1.RateAmount, red.quantity, newquatity, item33.item_qty, red.act_qty, red.case_qtyitem);
                                                                    red.extdcost = myswitch.Item1; newquatity = myswitch.Item3; red.quantity = myswitch.Item3;

                                                                    if (foundRatecal.CalculRate == 44)
                                                                    {
                                                                        if (forflat == 1)
                                                                        {
                                                                            red.quantity = 1;
                                                                            newquatity = 1;
                                                                            red.guid = item1.RateAmount;

                                                                        }
                                                                        else
                                                                        {
                                                                            red.quantity = 0;
                                                                            red.oldunitcost = 0;
                                                                            newquatity = 0;
                                                                        }
                                                                    }
                                                                    red.doc_id = foundNameabrg.NameAbrg;
                                                                    if ((mygroup.Total * item1.RateAmount) < red.oldunitcost)
                                                                    {
                                                                        if (forflat == 1 && foundRatecal.CalculRate == 44)
                                                                        {

                                                                        }
                                                                        else
                                                                        {
                                                                            red.guid = item1.SettingMin;
                                                                        }
                                                                    }


                                                                    else
                                                                    {
                                                                        if (item33.emp_num.Trim().ToUpper() == "CONT")
                                                                        {
                                                                            red.guid = 0;
                                                                            red.quantity = 0;
                                                                        }
                                                                        else
                                                                        {
                                                                            red.guid = item1.RateAmount * newquatity;
                                                                        }

                                                                    }

                                                                    int nassira = 0;
                                                                    if (inout == 2)
                                                                    {
                                                                        if (mygroup.Remotekey != wromote || mygroup.posuffix != wsuffp)
                                                                        {

                                                                            var pickTable = (from c in db.picks
                                                                                             where c.order_ == mygroup.Remotekey && c.order_suffix == mygroup.posuffix && c.abs_num.ToUpper() == mygroup.Rabsnum.ToUpper()
                                                                                             group c by new { c.order_, c.order_suffix, abs_num = c.abs_num.ToUpper(), c.lot, c.bin_num } into d
                                                                                             select new
                                                                                             {
                                                                                                 Remotekey = d.Key.order_,
                                                                                                 Rabsnum = d.Key.abs_num,
                                                                                                 posuffix = d.Key.order_suffix,
                                                                                                 wlot = d.Key.lot,
                                                                                                 wbin = d.Key.bin_num,
                                                                                                 Total = d.Sum(x => x.qty)

                                                                                             }).OrderBy(x => x.Remotekey).ToList();

                                                                            if (foundRatecal.CalculRate == 262)
                                                                            {
                                                                                var pickTable2 = (from c in db.picks
                                                                                                  join k in db.items on new { c.co_num, c.wh_num, abs_num = c.abs_num.ToUpper() } equals new { k.co_num, k.wh_num, abs_num = k.abs_num.ToUpper() }
                                                                                                  where c.order_ == mygroup.Remotekey && c.order_suffix == mygroup.posuffix && c.abs_num.ToUpper() == mygroup.Rabsnum.ToUpper()
                                                                                                  group c by new { c.order_, c.order_suffix, abs_num = c.abs_num.ToUpper(), c.bin_num } into d
                                                                                                  select new
                                                                                                  {
                                                                                                      Remotekey = d.Key.order_,
                                                                                                      Rabsnum = d.Key.abs_num,
                                                                                                      posuffix = d.Key.order_suffix,
                                                                                                      binnum = d.Key.bin_num,
                                                                                                      Total = d.Sum(x => x.qty)

                                                                                                  }).OrderBy(x => x.Remotekey).ToList();
                                                                                foreach (var pictabl in pickTable2)
                                                                                {
                                                                                    AudiViewModel red2 = new AudiViewModel();
                                                                                    red2.abs_num = red.abs_num;
                                                                                    var res80 = db.items.FirstOrDefault(x => x.abs_num.ToUpper() == pictabl.Rabsnum.ToUpper() && x.co_num == conum && x.wh_num == whnum);
                                                                                    try
                                                                                    {
                                                                                        var quq = pictabl.Total / res80.pallet_qty;
                                                                                        var oo = Convert.ToDecimal(quq);
                                                                                        var go = CalculHandling.TruncDecimal(oo, 2);
                                                                                        var go1 = CalculHandling.TruncDecimal1(oo, 2);
                                                                                        if (go1 == 0 && pictabl.Total < res80.pallet_qty)
                                                                                        {
                                                                                            newquatity = pictabl.Total;
                                                                                        }
                                                                                        if (go1 == 0 && pictabl.Total >= res80.pallet_qty)
                                                                                        {
                                                                                            newquatity = 0;
                                                                                        }
                                                                                        if (go1 >= 1 && pictabl.Total < res80.pallet_qty)
                                                                                        {
                                                                                            newquatity = pictabl.Total;
                                                                                        }
                                                                                        if (go1 >= 1 && pictabl.Total > res80.pallet_qty)
                                                                                        {
                                                                                            newquatity = pictabl.Total - (res80.pallet_qty * go);
                                                                                        }
                                                                                        
                                                                                    }
                                                                                    catch (Exception)
                                                                                    {


                                                                                    }

                                                                                    red2.act_qty = red.act_qty;
                                                                                    red2.action_code = red.action_code;
                                                                                    red2.adj_code = red.adj_code;
                                                                                    red2.batch = red.batch;
                                                                                    red2.bin_from = red.bin_from;
                                                                                    red2.bin_num = red.bin_num;
                                                                                    red2.bin_to = red.bin_to;
                                                                                    red2.box_qtyitem = red.box_qtyitem;
                                                                                    red2.cancelled = red.cancelled;
                                                                                    red2.cancelled_at = red.cancelled_at;
                                                                                    red2.cancelled_by = red.cancelled_by;
                                                                                    red2.cargo_control = red.cargo_control;
                                                                                    red2.carton_id = red.carton_id;
                                                                                    red2.case_qty = red.case_qty;
                                                                                    red2.case_qtyitem = red.case_qtyitem;
                                                                                    red2.cc_string = red.cc_string;
                                                                                    red2.cc_type = red.cc_type;
                                                                                    red2.co_num = red.co_num;
                                                                                    red2.comments = red.comments;
                                                                                    red2.country_code = red.country_code;
                                                                                    red2.date_time = red.date_time;
                                                                                    red2.date_time2 = red.date_time2;
                                                                                    red2.doc_id = red.doc_id;
                                                                                    red2.emp_num = red.emp_num;
                                                                                    red2.extdcost = red.extdcost;
                                                                                    red2.fromdatetime = red.fromdatetime;
                                                                                    red2.guid = red.guid;
                                                                                    red2.item_num = red.item_num;
                                                                                    red2.item_qty = red.item_qty;
                                                                                    red2.item_type = red.item_type;
                                                                                    red2.line_sequence = red.line_sequence;
                                                                                    red2.lot = red.lot;
                                                                                    red2.msg_status = red.msg_status;
                                                                                    red2.old_stock_stat = red.old_stock_stat;
                                                                                    red2.oldextdcost = red.oldextdcost;
                                                                                    red2.oldunitcost = red.oldunitcost;
                                                                                    red2.pallet_id = red.pallet_id;
                                                                                    red2.pallet_id_from = red.pallet_id_from;
                                                                                    red2.po_line = red.po_line;
                                                                                    red2.po_number = red.po_number;
                                                                                    red2.po_suffix = red.po_suffix;
                                                                                    red2.pool = red.pool;
                                                                                    red2.qa_release_id = red.qa_release_id;
                                                                                    red2.transmission = red.transmission;
                                                                                    red2.release_id = red.release_id;
                                                                                    red2.result_code = red.result_code;
                                                                                    red2.result_msg = red.result_msg;
                                                                                    red2.row_status = red.row_status;
                                                                                    red2.sugg_qty = red.sugg_qty;
                                                                                    red2.todatetime = red.todatetime;
                                                                                    red2.trans_type = red.trans_type;
                                                                                    red2.unitcost = red.unitcost;
                                                                                    red2.uom = red.uom;
                                                                                    red2.wh_num = red.wh_num;
                                                                                    nassira = 1;
                                                                                    red2.quantity = newquatity;
                                                                                    red.substat_code = item1.RateDescId;
                                                                                    red2.guid = item1.RateAmount * red2.quantity;
                                                                                    if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                                                                    {
                                                                                        red.guid = item1.SettingMax;
                                                                                    }
                                                                                    if (red.guid < item1.SettingMin && item1.SettingMin != 0)
                                                                                    {
                                                                                        red.guid = item1.SettingMin;
                                                                                    }
                                                                                    mylistfinal.Add(red2);
                                                                                }
                                                                            }

                                                                            else if (foundRatecal.CalculRate != 11)
                                                                            {
                                                                                decimal? newcal = 0, newcal1 = 0;
                                                                                if (foundRatecal.CalculRate != 12)
                                                                                {

                                                                                    var pickTable15 = (from c in db.picks
                                                                                                       where c.order_ == mygroup.Remotekey && c.order_suffix == mygroup.posuffix && c.abs_num == mygroup.Rabsnum
                                                                                                       group c by new { c.order_, c.order_suffix, abs_num = c.abs_num.ToUpper() } into d
                                                                                                       select new
                                                                                                       {
                                                                                                           Remotekey = d.Key.order_,
                                                                                                           Rabsnum = d.Key.abs_num,
                                                                                                           posuffix = d.Key.order_suffix,
                                                                                                           Total = d.Sum(x => x.qty)

                                                                                                       }).OrderBy(x => x.Remotekey).ToList();

                                                                                    if (pickTable15.Count > 1)
                                                                                    {
                                                                                        if (foundRatecal.CalculRate == 33)
                                                                                        {

                                                                                        }
                                                                                        else
                                                                                        {
                                                                                            foreach (var pictabl in pickTable15)
                                                                                            {
                                                                                                AudiViewModel red2 = new AudiViewModel();
                                                                                                red2.abs_num = red.abs_num;
                                                                                                red2.act_qty = red.act_qty;
                                                                                                red2.action_code = red.action_code;
                                                                                                red2.adj_code = red.adj_code;
                                                                                                red2.batch = red.batch;
                                                                                                red2.bin_from = red.bin_from;
                                                                                                red2.bin_num = red.bin_num;
                                                                                                red2.bin_to = red.bin_to;
                                                                                                red2.box_qtyitem = red.box_qtyitem;
                                                                                                red2.cancelled = red.cancelled;
                                                                                                red2.cancelled_at = red.cancelled_at;
                                                                                                red2.cancelled_by = red.cancelled_by;
                                                                                                red2.cargo_control = red.cargo_control;
                                                                                                red2.carton_id = red.carton_id;
                                                                                                red2.case_qty = red.case_qty;
                                                                                                red2.case_qtyitem = red.case_qtyitem;
                                                                                                red2.cc_string = red.cc_string;
                                                                                                red2.cc_type = red.cc_type;
                                                                                                red2.co_num = red.co_num;
                                                                                                red2.comments = red.comments;
                                                                                                red2.country_code = red.country_code;
                                                                                                red2.date_time = red.date_time;
                                                                                                red2.date_time2 = red.date_time2;
                                                                                                red2.doc_id = red.doc_id;
                                                                                                red2.emp_num = red.emp_num;
                                                                                                red2.extdcost = red.extdcost;
                                                                                                red2.fromdatetime = red.fromdatetime;
                                                                                                red2.guid = red.guid;
                                                                                                red2.item_num = red.item_num;
                                                                                                red2.item_qty = red.item_qty;
                                                                                                red2.item_type = red.item_type;
                                                                                                red2.line_sequence = red.line_sequence;
                                                                                                red2.lot = red.lot;
                                                                                                red2.msg_status = red.msg_status;
                                                                                                red2.old_stock_stat = red.old_stock_stat;
                                                                                                red2.oldextdcost = red.oldextdcost;
                                                                                                red2.oldunitcost = red.oldunitcost;
                                                                                                red2.pallet_id = red.pallet_id;
                                                                                                red2.pallet_id_from = red.pallet_id_from;
                                                                                                red2.po_line = red.po_line;
                                                                                                red2.po_number = red.po_number;
                                                                                                red2.po_suffix = red.po_suffix;
                                                                                                red2.pool = red.pool;
                                                                                                red2.qa_release_id = red.qa_release_id;
                                                                                                red2.transmission = red.transmission;
                                                                                                red2.release_id = red.release_id;
                                                                                                red2.result_code = red.result_code;
                                                                                                red2.result_msg = red.result_msg;
                                                                                                red2.row_status = red.row_status;
                                                                                                red2.sugg_qty = red.sugg_qty;
                                                                                                red2.todatetime = red.todatetime;
                                                                                                red2.trans_type = red.trans_type;
                                                                                                red2.unitcost = red.unitcost;
                                                                                                red2.uom = red.uom;
                                                                                                red2.wh_num = red.wh_num;
                                                                                                nassira = 1;
                                                                                                calc = Convert.ToDecimal(pictabl.Total);
                                                                                                Tuple<decimal?, decimal?, decimal?> myswitch1 = CalculHandling.CalculRateFore(foundRatecal.CalculRate, forthesame, calc, red.extdcost, item1.RateAmount, pictabl.Total, pictabl.Total, item33.item_qty, red.act_qty, red.case_qtyitem);
                                                                                                red2.extdcost = myswitch.Item1; newquatity = myswitch1.Item3; red2.quantity = myswitch1.Item2;
                                                                                                red2.quantity = newquatity;
                                                                                                red.substat_code = item1.RateDescId;
                                                                                                if (newquatity != 0)
                                                                                                {
                                                                                                    if (item1.UnitCode.ToUpper() == "HR" || item1.UnitCode.ToUpper() == "HHR")
                                                                                                    {
                                                                                                        if (foundsettingcustomer != null)
                                                                                                        {
                                                                                                            if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                                                            if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                                                            if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                                                            hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                                                            red2.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red2.quantity);
                                                                                                        }
                                                                                                    }
                                                                                                }
                                                                                                red2.guid = item1.RateAmount * red2.quantity;
                                                                                                newcal += red2.guid;
                                                                                                newcal1 = red2.oldunitcost;
                                                                                                if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                                                                                {
                                                                                                    red.guid = item1.SettingMax;
                                                                                                }
                                                                                                mylistfinal.Add(red2);
                                                                                            }
                                                                                        }
                                                                                        if (newcal1 > newcal)
                                                                                        {
                                                                                            AudiViewModel red2 = new AudiViewModel();
                                                                                            red2.abs_num = red.abs_num;
                                                                                            red2.act_qty = red.act_qty;
                                                                                            red2.action_code = red.action_code;
                                                                                            red2.adj_code = red.adj_code;
                                                                                            red2.batch = red.batch;
                                                                                            red2.bin_from = red.bin_from;
                                                                                            red2.bin_num = red.bin_num;
                                                                                            red2.bin_to = red.bin_to;
                                                                                            red2.box_qtyitem = red.box_qtyitem;
                                                                                            red2.cancelled = red.cancelled;
                                                                                            red2.cancelled_at = red.cancelled_at;
                                                                                            red2.cancelled_by = red.cancelled_by;
                                                                                            red2.cargo_control = red.cargo_control;
                                                                                            red2.carton_id = red.carton_id;
                                                                                            red2.case_qty = red.case_qty;
                                                                                            red2.case_qtyitem = red.case_qtyitem;
                                                                                            red2.transmission = red.transmission;
                                                                                            red2.cc_string = red.cc_string;
                                                                                            red2.cc_type = red.cc_type;
                                                                                            red2.co_num = red.co_num;
                                                                                            red2.comments = red.comments;
                                                                                            red2.country_code = red.country_code;
                                                                                            red2.date_time = red.date_time;
                                                                                            red2.date_time2 = red.date_time2;
                                                                                            red2.doc_id = red.doc_id;
                                                                                            red2.emp_num = red.emp_num;
                                                                                            red2.extdcost = red.extdcost;
                                                                                            red2.fromdatetime = red.fromdatetime;
                                                                                            red2.guid = red.guid;
                                                                                            red2.item_num = red.item_num;
                                                                                            red2.item_qty = red.item_qty;
                                                                                            red2.item_type = red.item_type;
                                                                                            red2.line_sequence = red.line_sequence;
                                                                                            red2.lot = red.lot;
                                                                                            red2.msg_status = red.msg_status;
                                                                                            red2.old_stock_stat = red.old_stock_stat;
                                                                                            red2.oldextdcost = red.oldextdcost;
                                                                                            red2.oldunitcost = red.oldunitcost;
                                                                                            red2.pallet_id = red.pallet_id;
                                                                                            red2.pallet_id_from = red.pallet_id_from;
                                                                                            red2.po_line = red.po_line;
                                                                                            red2.po_number = red.po_number;
                                                                                            red2.po_suffix = red.po_suffix;
                                                                                            red2.pool = red.pool;
                                                                                            red2.qa_release_id = red2.qa_release_id;

                                                                                            red2.release_id = red.release_id;
                                                                                            red2.result_code = red.result_code;
                                                                                            red2.result_msg = red.result_msg;
                                                                                            red2.row_status = red.row_status;
                                                                                            red2.sugg_qty = red.sugg_qty;
                                                                                            red2.todatetime = red.todatetime;
                                                                                            red2.trans_type = red.trans_type;
                                                                                            red2.unitcost = red.unitcost;
                                                                                            red2.uom = red.uom;
                                                                                            red2.wh_num = red.wh_num;
                                                                                            red2.quantity = 1;
                                                                                            red.substat_code = item1.RateDescId;
                                                                                            red2.guid = item1.SettingMin - newcal;
                                                                                            if (red2.guid > item1.SettingMax && item1.SettingMax != 0)
                                                                                            {
                                                                                                red2.guid = item1.SettingMax;
                                                                                            }
                                                                                            mylistfinal.Add(red2);
                                                                                        }
                                                                                    }
                                                                                }
                                                                                else
                                                                                {
                                                                                    if (pickTable.Count > 1)
                                                                                    {

                                                                                        foreach (var pictabl in pickTable)
                                                                                        {
                                                                                            AudiViewModel red2 = new AudiViewModel();
                                                                                            red2.abs_num = red.abs_num;
                                                                                            red2.act_qty = red.act_qty;
                                                                                            red2.action_code = red.action_code;
                                                                                            red2.adj_code = red.adj_code;
                                                                                            red2.batch = red.batch;
                                                                                            red2.bin_from = red.bin_from;
                                                                                            red2.bin_num = red.bin_num;
                                                                                            red2.bin_to = red.bin_to;
                                                                                            red2.box_qtyitem = red.box_qtyitem;
                                                                                            red2.cancelled = red.cancelled;
                                                                                            red2.cancelled_at = red.cancelled_at;
                                                                                            red2.cancelled_by = red.cancelled_by;
                                                                                            red2.cargo_control = red.cargo_control;
                                                                                            red2.carton_id = red.carton_id;
                                                                                            red2.case_qty = red.case_qty;
                                                                                            red2.case_qtyitem = red.case_qtyitem;
                                                                                            red2.cc_string = red.cc_string;
                                                                                            red2.cc_type = red.cc_type;
                                                                                            red2.co_num = red.co_num;
                                                                                            red2.comments = red.comments;
                                                                                            red2.country_code = red.country_code;
                                                                                            red2.date_time = red.date_time;
                                                                                            red2.date_time2 = red.date_time2;
                                                                                            red2.doc_id = red.doc_id;
                                                                                            red2.emp_num = red.emp_num;
                                                                                            red2.extdcost = red.extdcost;
                                                                                            red2.fromdatetime = red.fromdatetime;
                                                                                            red2.guid = red.guid;
                                                                                            red2.item_num = red.item_num;
                                                                                            red2.item_qty = red.item_qty;
                                                                                            red2.item_type = red.item_type;
                                                                                            red2.line_sequence = red.line_sequence;
                                                                                            red2.lot = red.lot;
                                                                                            red2.msg_status = red.msg_status;
                                                                                            red2.old_stock_stat = red.old_stock_stat;
                                                                                            red2.oldextdcost = red.oldextdcost;
                                                                                            red2.oldunitcost = red.oldunitcost;
                                                                                            red2.pallet_id = red.pallet_id;
                                                                                            red2.pallet_id_from = red.pallet_id_from;
                                                                                            red2.po_line = red.po_line;
                                                                                            red2.po_number = red.po_number;
                                                                                            red2.po_suffix = red.po_suffix;
                                                                                            red2.pool = red.pool;
                                                                                            red2.qa_release_id = red.qa_release_id;
                                                                                            red2.transmission = red.transmission;
                                                                                            red2.release_id = red.release_id;
                                                                                            red2.result_code = red.result_code;
                                                                                            red2.result_msg = red.result_msg;
                                                                                            red2.row_status = red.row_status;
                                                                                            red2.sugg_qty = red.sugg_qty;
                                                                                            red2.todatetime = red.todatetime;
                                                                                            red2.trans_type = red.trans_type;
                                                                                            red2.unitcost = red.unitcost;
                                                                                            red2.uom = red.uom;
                                                                                            red2.wh_num = red.wh_num;
                                                                                            nassira = 1;
                                                                                            calc = Convert.ToDecimal(pictabl.Total);
                                                                                            Tuple<decimal?, decimal?, decimal?> myswitch1 = CalculHandling.CalculRateFore(foundRatecal.CalculRate, forthesame, calc, red.extdcost, item1.RateAmount, pictabl.Total, pictabl.Total, item33.item_qty, red.act_qty, red.case_qtyitem);
                                                                                            red2.extdcost = myswitch.Item1; newquatity = myswitch1.Item3; red2.quantity = myswitch1.Item2;
                                                                                            red2.quantity = newquatity;
                                                                                            red.substat_code = item1.RateDescId;
                                                                                            if (newquatity != 0)
                                                                                            {
                                                                                                if (item1.UnitCode.ToUpper() == "HR" || item1.UnitCode.ToUpper() == "HHR")
                                                                                                {
                                                                                                    if (foundsettingcustomer != null)
                                                                                                    {
                                                                                                        if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                                                        if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                                                        if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                                                        hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                                                        red2.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red2.quantity);
                                                                                                    }
                                                                                                }
                                                                                            }
                                                                                            red2.guid = item1.RateAmount * red2.quantity;
                                                                                            newcal += red2.guid;
                                                                                            newcal1 = red2.oldunitcost;
                                                                                            if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                                                                            {
                                                                                                red.guid = item1.SettingMax;
                                                                                            }
                                                                                            mylistfinal.Add(red2);
                                                                                        }

                                                                                        if (newcal1 > newcal)
                                                                                        {
                                                                                            AudiViewModel red2 = new AudiViewModel();
                                                                                            red2.abs_num = red.abs_num;
                                                                                            red2.act_qty = red.act_qty;
                                                                                            red2.action_code = red.action_code;
                                                                                            red2.adj_code = red.adj_code;
                                                                                            red2.batch = red.batch;
                                                                                            red2.bin_from = red.bin_from;
                                                                                            red2.bin_num = red.bin_num;
                                                                                            red2.bin_to = red.bin_to;
                                                                                            red2.box_qtyitem = red.box_qtyitem;
                                                                                            red2.cancelled = red.cancelled;
                                                                                            red2.cancelled_at = red.cancelled_at;
                                                                                            red2.cancelled_by = red.cancelled_by;
                                                                                            red2.cargo_control = red.cargo_control;
                                                                                            red2.carton_id = red.carton_id;
                                                                                            red2.case_qty = red.case_qty;
                                                                                            red2.case_qtyitem = red.case_qtyitem;
                                                                                            red2.transmission = red.transmission;
                                                                                            red2.cc_string = red.cc_string;
                                                                                            red2.cc_type = red.cc_type;
                                                                                            red2.co_num = red.co_num;
                                                                                            red2.comments = red.comments;
                                                                                            red2.country_code = red.country_code;
                                                                                            red2.date_time = red.date_time;
                                                                                            red2.date_time2 = red.date_time2;
                                                                                            red2.doc_id = red.doc_id;
                                                                                            red2.emp_num = red.emp_num;
                                                                                            red2.extdcost = red.extdcost;
                                                                                            red2.fromdatetime = red.fromdatetime;
                                                                                            red2.guid = red.guid;
                                                                                            red2.item_num = red.item_num;
                                                                                            red2.item_qty = red.item_qty;
                                                                                            red2.item_type = red.item_type;
                                                                                            red2.line_sequence = red.line_sequence;
                                                                                            red2.lot = red.lot;
                                                                                            red2.msg_status = red.msg_status;
                                                                                            red2.old_stock_stat = red.old_stock_stat;
                                                                                            red2.oldextdcost = red.oldextdcost;
                                                                                            red2.oldunitcost = red.oldunitcost;
                                                                                            red2.pallet_id = red.pallet_id;
                                                                                            red2.pallet_id_from = red.pallet_id_from;
                                                                                            red2.po_line = red.po_line;
                                                                                            red2.po_number = red.po_number;
                                                                                            red2.po_suffix = red.po_suffix;
                                                                                            red2.pool = red.pool;
                                                                                            red2.qa_release_id = red2.qa_release_id;

                                                                                            red2.release_id = red.release_id;
                                                                                            red2.result_code = red.result_code;
                                                                                            red2.result_msg = red.result_msg;
                                                                                            red2.row_status = red.row_status;
                                                                                            red2.sugg_qty = red.sugg_qty;
                                                                                            red2.todatetime = red.todatetime;
                                                                                            red2.trans_type = red.trans_type;
                                                                                            red2.unitcost = red.unitcost;
                                                                                            red2.uom = red.uom;
                                                                                            red2.wh_num = red.wh_num;
                                                                                            red2.quantity = 1;
                                                                                            red.substat_code = item1.RateDescId;
                                                                                            red2.guid = item1.SettingMin - newcal;
                                                                                            if (red2.guid > item1.SettingMax && item1.SettingMax != 0)
                                                                                            {
                                                                                                red2.guid = item1.SettingMax;
                                                                                            }
                                                                                            mylistfinal.Add(red2);
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }
                                                                            else
                                                                            {
                                                                                red.quantity = 0;
                                                                            }
                                                                            wromote = mygroup.Remotekey; wsuffp = mygroup.posuffix; wabss = mygroup.Rabsnum;

                                                                        }
                                                                        else
                                                                        {
                                                                            nassira = 1;
                                                                        }
                                                                    }
                                                                    if (nassira != 1)
                                                                    {

                                                                        if (item1.RateAmount > 0)
                                                                        {
                                                                            if (red.guid < red.oldunitcost)
                                                                            {
                                                                                red.guid = red.oldunitcost;
                                                                            }
                                                                        }
                                                                        mylistfinal.Add(red);
                                                                    }

                                                                }
                                                            }
                                                        }
                                                        else
                                                        {

                                                            foreach (var item33 in mylistgo)
                                                            {
                                                                newquatity = item33.quantity;
                                                                forthesame += 1;

                                                                try
                                                                {
                                                                    var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item1.RateDescId);
                                                                    if (df.ForGroup == 1)
                                                                    {
                                                                        var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item1.RateDescId && x.GroupName.Trim() == item33.emp_num.Trim() && x.KindId == inout).FirstOrDefault();
                                                                        item1.RateAmount = foundrateforGroup.GroupRate;
                                                                    }
                                                                }
                                                                catch (Exception)
                                                                {
                                                                    if (item33.emp_num.Length != 0) { item1.RateAmount = 0; item1.SettingMin = 0; }

                                                                }
                                                                if (item1.abs_num != item33.abs_num)
                                                                {
                                                                    zalanew += 1;
                                                                    AudiViewModel red = new AudiViewModel();
                                                                    List<InvoiceViewModel> newinvoice = mylistinvoice;
                                                                    if (foundRatecal.PalletId != 0)
                                                                    {
                                                                        if (zalanew == 1)
                                                                        {
                                                                            newquatity = CalculHandling.CalculPaletId(conum, whnum, newinvoice, mygroup.Remotekey, mygroup.posuffix, mygroup.Rabsnum, newquatity);
                                                                        }
                                                                        else
                                                                        {
                                                                            newquatity = 0; red.release_id = "N";
                                                                        }
                                                                    }
                                                                    else
                                                                    {

                                                                        string wabs = mygroup.Rabsnum;
                                                                        newquatity = CalculHandling.CalculFirst(foundRatecal.CalculRate, newinvoice, mygroup.Remotekey, mygroup.posuffix, wabs, newquatity);


                                                                    }

                                                                    red.wh_num = whnum;
                                                                    red.co_num = conum;
                                                                    red.substat_code = item1.RateDescId;
                                                                    red.oldunitcost = item1.SettingMin;
                                                                    red.unitcost = item1.RateAmount;
                                                                    red.transmission = item1.SettingId;
                                                                    if (item33.emp_num.Trim().ToUpper() == "CONT")
                                                                    {
                                                                        red.unitcost = 0;
                                                                        red.oldunitcost = 0;
                                                                    }

                                                                    red.fromdatetime = item1.ChargeName; red.po_number = item33.po_number; red.po_suffix = item33.po_suffix; red.abs_num = item33.abs_num;
                                                                    red.po_line = item33.po_line; red.lot = item33.lot; red.row_status = item33.row_status; red.sugg_qty = item33.quantity;
                                                                    red.act_qty = item33.act_qty; red.item_type = item33.item_type; red.date_time2 = item33.date_time2; red.emp_num = item33.emp_num;
                                                                    red.result_msg = item33.result_msg; red.case_qtyitem = item33.case_qtyitem; red.box_qtyitem = item33.box_qtyitem;
                                                                    itemqty = item33.item_qty; red.cc_type = item33.cc_type; red.action_code = item1.UnitCode;
                                                                    decimal calc = CalculHandling.CalculSecond(foundRatecal.CalculRate, foundRatecal.PalletId, newquatity, itemqty);
                                                                    Tuple<decimal?, decimal?> myres = CalculHandling.CalculRate(foundRatecal.CalculRate, calc, newquatity, red.sugg_qty);
                                                                    calc = Convert.ToDecimal(myres.Item1);
                                                                    newquatity = myres.Item2;

                                                                    Tuple<decimal?, decimal?, decimal?> myswitch = CalculHandling.CalculRateFore(foundRatecal.CalculRate, forthesame, calc, red.extdcost, item1.RateAmount, red.quantity, newquatity, item33.item_qty, red.act_qty, red.case_qtyitem);
                                                                    red.extdcost = myswitch.Item1; newquatity = myswitch.Item3; red.quantity = myswitch.Item3;

                                                                    red.doc_id = foundNameabrg.NameAbrg;
                                                                    if (newquatity != 0)
                                                                    {
                                                                        if (item1.UnitCode.ToUpper() == "HR" || item1.UnitCode.ToUpper() == "HHR")
                                                                        {
                                                                            if (foundsettingcustomer != null)
                                                                            {
                                                                                if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                                if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                                if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                                hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                                newquatity = Getquantity("HR", hincrem, hourlymin, (decimal)newquatity);
                                                                            }
                                                                        }
                                                                    }
                                                                    if ((newquatity * item1.RateAmount) < red.oldunitcost)
                                                                    {
                                                                        red.guid = item1.SettingMin;
                                                                    }

                                                                    else
                                                                    {
                                                                        if (item33.emp_num.Trim().ToUpper() == "CONT")
                                                                        {
                                                                            red.guid = 0;
                                                                        }
                                                                        else
                                                                        {
                                                                            red.guid = item1.RateAmount * newquatity;
                                                                        }

                                                                    }
                                                                    
                                                                    int nassira = 0;
                                                                    if (inout == 2)
                                                                    {
                                                                        if (mygroup.Remotekey != wromote || mygroup.posuffix != wsuffp)
                                                                        {
                                                                            var pickTable = (from c in db.picks
                                                                                             where c.order_ == mygroup.Remotekey && c.order_suffix == mygroup.posuffix && c.abs_num == mygroup.Rabsnum
                                                                                             group c by new { c.order_, c.order_suffix, abs_num = c.abs_num.ToUpper(), c.lot, c.bin_num } into d
                                                                                             select new
                                                                                             {
                                                                                                 Remotekey = d.Key.order_,
                                                                                                 Rabsnum = d.Key.abs_num,
                                                                                                 posuffix = d.Key.order_suffix,
                                                                                                 wlot = d.Key.lot,
                                                                                                 wbin = d.Key.bin_num,
                                                                                                 Total = d.Sum(x => x.qty)

                                                                                             }).OrderBy(x => x.Remotekey).ToList();

                                                                            if (foundRatecal.CalculRate != 11)
                                                                            {
                                                                                decimal? newcal = 0, newcal1 = 0;
                                                                                if (pickTable.Count > 1)
                                                                                {

                                                                                    foreach (var pictabl in pickTable)
                                                                                    {
                                                                                        AudiViewModel red2 = new AudiViewModel();
                                                                                        red2.abs_num = red.abs_num;
                                                                                        red2.act_qty = red.act_qty;
                                                                                        red2.action_code = red.action_code;
                                                                                        red2.adj_code = red.adj_code;
                                                                                        red2.batch = red.batch;
                                                                                        red2.bin_from = red.bin_from;
                                                                                        red2.bin_num = red.bin_num;
                                                                                        red2.bin_to = red.bin_to;
                                                                                        red2.box_qtyitem = red.box_qtyitem;
                                                                                        red2.cancelled = red.cancelled;
                                                                                        red2.cancelled_at = red.cancelled_at;
                                                                                        red2.cancelled_by = red.cancelled_by;
                                                                                        red2.cargo_control = red.cargo_control;
                                                                                        red2.transmission = red.transmission;
                                                                                        red2.carton_id = red.carton_id;
                                                                                        red2.case_qty = red.case_qty;
                                                                                        red2.case_qtyitem = red.case_qtyitem;
                                                                                        red2.cc_string = red.cc_string;
                                                                                        red2.cc_type = red.cc_type;
                                                                                        red2.co_num = red.co_num;
                                                                                        red2.comments = red.comments;
                                                                                        red2.country_code = red.country_code;
                                                                                        red2.date_time = red.date_time;
                                                                                        red2.date_time2 = red.date_time2;
                                                                                        red2.doc_id = red.doc_id;
                                                                                        red2.emp_num = red.emp_num;
                                                                                        red2.extdcost = red.extdcost;
                                                                                        red2.fromdatetime = red.fromdatetime;
                                                                                        red2.guid = red.guid;
                                                                                        red2.item_num = red.item_num;
                                                                                        red2.item_qty = red.item_qty;
                                                                                        red2.item_type = red.item_type;
                                                                                        red2.line_sequence = red.line_sequence;
                                                                                        red2.lot = red.lot;
                                                                                        red2.msg_status = red.msg_status;
                                                                                        red2.old_stock_stat = red.old_stock_stat;
                                                                                        red2.oldextdcost = red.oldextdcost;
                                                                                        red2.oldunitcost = red.oldunitcost;
                                                                                        red2.pallet_id = red.pallet_id;
                                                                                        red2.pallet_id_from = red.pallet_id_from;
                                                                                        red2.po_line = red.po_line;
                                                                                        red2.po_number = red.po_number;
                                                                                        red2.po_suffix = red.po_suffix;
                                                                                        red2.pool = red.pool;
                                                                                        red2.qa_release_id = red2.qa_release_id;

                                                                                        red2.release_id = red.release_id;
                                                                                        red2.result_code = red.result_code;
                                                                                        red2.result_msg = red.result_msg;
                                                                                        red2.row_status = red.row_status;
                                                                                        red2.sugg_qty = red.sugg_qty;
                                                                                        red2.todatetime = red.todatetime;
                                                                                        red2.trans_type = red.trans_type;
                                                                                        red2.unitcost = red.unitcost;
                                                                                        red2.uom = red.uom;
                                                                                        red2.wh_num = red.wh_num;
                                                                                        nassira = 1;
                                                                                        calc = Convert.ToDecimal(pictabl.Total);
                                                                                        Tuple<decimal?, decimal?, decimal?> myswitch1 = CalculHandling.CalculRateFore(foundRatecal.CalculRate, forthesame, calc, red.extdcost, item1.RateAmount, pictabl.Total, pictabl.Total, item33.item_qty, red.act_qty, red.case_qtyitem);
                                                                                        red2.extdcost = myswitch.Item1; newquatity = myswitch1.Item3; red2.quantity = myswitch1.Item2;
                                                                                        red2.quantity = newquatity;
                                                                                        red.substat_code = item1.RateDescId;
                                                                                        if (newquatity != 0)
                                                                                        {
                                                                                            if (item1.UnitCode.ToUpper() == "HR" || item1.UnitCode.ToUpper() == "HHR")
                                                                                            {
                                                                                                if (foundsettingcustomer != null)
                                                                                                {
                                                                                                    if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                                                    if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                                                    if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                                                    hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                                                    red2.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red2.quantity);
                                                                                                }
                                                                                            }
                                                                                        }
                                                                                        red2.guid = item1.RateAmount * red2.quantity;
                                                                                        newcal += red2.guid;
                                                                                        newcal1 = red2.oldunitcost;
                                                                                       
                                                                                        mylistfinal5.Add(red2);
                                                                                    }

                                                                                    if (newcal1 > newcal)
                                                                                    {
                                                                                        AudiViewModel red2 = new AudiViewModel();
                                                                                        red2.abs_num = red.abs_num;
                                                                                        red2.act_qty = red.act_qty;
                                                                                        red2.action_code = red.action_code;
                                                                                        red2.adj_code = red.adj_code;
                                                                                        red2.batch = red.batch;
                                                                                        red2.bin_from = red.bin_from;
                                                                                        red2.bin_num = red.bin_num;
                                                                                        red2.bin_to = red.bin_to;
                                                                                        red2.box_qtyitem = red.box_qtyitem;
                                                                                        red2.cancelled = red.cancelled;
                                                                                        red2.cancelled_at = red.cancelled_at;
                                                                                        red2.cancelled_by = red.cancelled_by;
                                                                                        red2.cargo_control = red.cargo_control;
                                                                                        red2.carton_id = red.carton_id;
                                                                                        red2.case_qty = red.case_qty;
                                                                                        red2.case_qtyitem = red.case_qtyitem;
                                                                                        red2.transmission = red.transmission;
                                                                                        red2.cc_string = red.cc_string;
                                                                                        red2.cc_type = red.cc_type;
                                                                                        red2.co_num = red.co_num;
                                                                                        red2.comments = red.comments;
                                                                                        red2.country_code = red.country_code;
                                                                                        red2.date_time = red.date_time;
                                                                                        red2.date_time2 = red.date_time2;
                                                                                        red2.doc_id = red.doc_id;
                                                                                        red2.emp_num = red.emp_num;
                                                                                        red2.extdcost = red.extdcost;
                                                                                        red2.fromdatetime = red.fromdatetime;
                                                                                        red2.guid = red.guid;
                                                                                        red2.item_num = red.item_num;
                                                                                        red2.item_qty = red.item_qty;
                                                                                        red2.item_type = red.item_type;
                                                                                        red2.line_sequence = red.line_sequence;
                                                                                        red2.lot = red.lot;
                                                                                        red2.msg_status = red.msg_status;
                                                                                        red2.old_stock_stat = red.old_stock_stat;
                                                                                        red2.oldextdcost = red.oldextdcost;
                                                                                        red2.oldunitcost = red.oldunitcost;
                                                                                        red2.pallet_id = red.pallet_id;
                                                                                        red2.pallet_id_from = red.pallet_id_from;
                                                                                        red2.po_line = red.po_line;
                                                                                        red2.po_number = red.po_number;
                                                                                        red2.po_suffix = red.po_suffix;
                                                                                        red2.pool = red.pool;
                                                                                        red2.qa_release_id = red2.qa_release_id;

                                                                                        red2.release_id = red.release_id;
                                                                                        red2.result_code = red.result_code;
                                                                                        red2.result_msg = red.result_msg;
                                                                                        red2.row_status = red.row_status;
                                                                                        red2.sugg_qty = red.sugg_qty;
                                                                                        red2.todatetime = red.todatetime;
                                                                                        red2.trans_type = red.trans_type;
                                                                                        red2.unitcost = red.unitcost;
                                                                                        red2.uom = red.uom;
                                                                                        red2.wh_num = red.wh_num;
                                                                                        red2.quantity = 1;
                                                                                        red.substat_code = item1.RateDescId;
                                                                                        red2.guid = item1.SettingMin - newcal;
                                                                                        if (red2.guid > item1.SettingMax && item1.SettingMax != 0)
                                                                                        {
                                                                                            red2.guid = item1.SettingMax;
                                                                                        }
                                                                                        mylistfinal5.Add(red2);
                                                                                    }
                                                                                }
                                                                            }
                                                                            else
                                                                            {
                                                                                red.quantity = 0;
                                                                            }
                                                                            wromote = mygroup.Remotekey; wsuffp = mygroup.posuffix; wabss = mygroup.Rabsnum;

                                                                        }
                                                                        else
                                                                        {
                                                                            nassira = 1;
                                                                        }


                                                                    }
                                                                    if (nassira != 1)
                                                                    {

                                                                        mylistfinal5.Add(red);
                                                                    }

                                                                }
                                                            }


                                                        }
                                                    }
                                                }
                                            }
                                           
                                            
                                        }
                                    }
                                    else
                                    {

                                        addwpik += 1;
                                        
                                        foreach (var mygroup in groupPonew2)
                                        {
                                            item1.RateAmount = wamm.RateAmount;
                                            item1.abs_num = "";
                                            decimal? newquatity = mygroup.Total;
                                            var mylistgo = mylist.Where(x => x.po_number == mygroup.Remotekey && x.po_suffix == mygroup.posuffix).ToList().OrderBy(x => x.abs_num).ToList();
                                            int zalanew = 0, forthesame = 0, binnumb = 0;
                                            string wwabsnum = ""; forflat = 0;
                                            foreach (var item33 in mylistgo)
                                            {
                                                var listgroup = groupPonew.FirstOrDefault(x => x.Remotekey == mygroup.Remotekey && x.posuffix == mygroup.posuffix && x.Rabsnum.Trim() == item33.abs_num.Trim());
                                                if (listgroup.Total != 0)
                                                {
                                                    forflat = +1;
                                                    forthesame += 1;
                                                    if (foundRatecal.PalletId == 1) zalanew = 0;

                                                    try
                                                    {
                                                        var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item1.RateDescId);
                                                        if (df.ForGroup == 1)
                                                        {
                                                            var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item1.RateDescId && x.GroupName.Trim() == item33.emp_num.Trim() && x.KindId == inout).FirstOrDefault();
                                                            item1.RateAmount = foundrateforGroup.GroupRate;
                                                            if (wwabsnum == item33.abs_num)
                                                            {

                                                                item1.abs_num = item33.abs_num;
                                                            }
                                                            else
                                                            {
                                                                item1.abs_num = "";
                                                            }
                                                        }
                                                    }
                                                    catch (Exception)
                                                    {

                                                        if (item33.emp_num.Length != 0) { item1.RateAmount = 0; item1.SettingMin = 0; }
                                                    }
                                                    if (wwabsnum == item33.abs_num)
                                                    {

                                                        newquatity = 0;
                                                        item1.abs_num = item33.abs_num;
                                                    }
                                                    if (item1.abs_num != item33.abs_num)
                                                    {

                                                        zalanew += 1;
                                                        if (inout == 3) zalanew = 1;
                                                        AudiViewModel red = new AudiViewModel();
                                                        if (foundRatecal.PalletId != 0)
                                                        {
                                                            if (zalanew == 1)
                                                            {
                                                                binnumb = 0;

                                                                var searchpalletid = mylistinvoice
                                                                    .Where(x => x.po_number == mygroup.Remotekey && x.co_num == conum && x.wh_num == whnum
                                                                        && x.po_suffix == mygroup.posuffix && x.abs_num == item33.abs_num)
                                                                   .GroupBy(x => x.pallet_id)
                                                                    .ToList();
                                                                

                                                                newquatity = searchpalletid.Distinct().Count();

                                                                if (binnumb >= 1) newquatity = 0;
                                                                if (binnumb == 0) binnumb = 1;
                                                            }
                                                            else
                                                            {
                                                                newquatity = 0;
                                                                red.release_id = "N";
                                                            }
                                                        }
                                                        else
                                                        {
                                                            List<InvoiceViewModel> newinvoice = mylistinvoice;
                                                            string wabs = "999AAA999";
                                                            newquatity = CalculHandling.CalculFirst(foundRatecal.CalculRate, newinvoice, mygroup.Remotekey, mygroup.posuffix, wabs, newquatity);


                                                        }

                                                        red.wh_num = whnum;
                                                        red.co_num = conum;
                                                        red.substat_code = item1.RateDescId;
                                                        red.oldunitcost = item1.SettingMin;
                                                        red.unitcost = item1.RateAmount;
                                                        red.transmission = item1.SettingId;
                                                        red.action_code = item1.UnitCode;
                                                        if (!string.IsNullOrEmpty(item33.emp_num))
                                                        {


                                                            if (item33.emp_num.Trim().ToUpper() == "CONT")
                                                            {
                                                                red.unitcost = 0;
                                                            }
                                                        }
                                                        var resullt = groupPonew.Where(x => x.Remotekey == item33.po_number && x.posuffix == item33.po_suffix && x.Rabsnum == item33.abs_num.ToUpper()).FirstOrDefault();
                                                        red.fromdatetime = item1.ChargeName;
                                                        red.po_number = item33.po_number;
                                                        red.po_suffix = item33.po_suffix;
                                                        red.abs_num = item33.abs_num;
                                                        red.po_line = item33.po_line;
                                                        red.date_time2 = item33.date_time2;
                                                        red.lot = item33.lot;
                                                        red.row_status = item33.row_status;
                                                        red.sugg_qty = resullt.Total;
                                                        red.act_qty = item33.act_qty;
                                                        red.item_type = item33.item_type;
                                                        red.emp_num = item33.emp_num;
                                                        red.result_msg = item33.result_msg;
                                                        red.case_qtyitem = item33.case_qtyitem;
                                                        red.box_qtyitem = item33.box_qtyitem;
                                                        red.cc_type = item33.cc_type;
                                                        red.quantity = newquatity;

                                                        
                                                        itemqty = item33.item_qty;
                                                        if (foundRatecal.PalletId == 0)
                                                        {
                                                            decimal calc = CalculHandling.CalculSecond(foundRatecal.CalculRate, foundRatecal.PalletId, newquatity, itemqty);

                                                            Tuple<decimal?, decimal?> myres = CalculHandling.CalculRate(foundRatecal.CalculRate, calc, newquatity, red.sugg_qty);
                                                            calc = Convert.ToDecimal(myres.Item1);
                                                            newquatity = myres.Item2;

                                                            Tuple<decimal?, decimal?, decimal?> myswitch = CalculHandling.CalculRateFore(foundRatecal.CalculRate, forthesame, calc, red.extdcost, item1.RateAmount, red.quantity, newquatity, item33.item_qty, red.act_qty, red.case_qtyitem);

                                                            red.extdcost = myswitch.Item1;
                                                            newquatity = myswitch.Item3;
                                                            red.quantity = myswitch.Item2;
                                                        }
                                                        if (foundRatecal.CalculRate == 44)
                                                        {
                                                            if (forflat == 1)
                                                            {
                                                                red.quantity = 1;
                                                                newquatity = 1;
                                                                red.guid = item1.RateAmount;
                                                            }
                                                            else
                                                            {
                                                                red.quantity = 0;
                                                                red.oldunitcost = 0;
                                                                red.extdcost = 0;
                                                                red.guid = 0;
                                                                newquatity = 0;
                                                            }

                                                        }


                                                        red.doc_id = foundNameabrg.NameAbrg;
                                                        if (red.oldunitcost > red.extdcost)
                                                        {
                                                            red.guid = item1.SettingMin;

                                                        }
                                                        else
                                                        {
                                                            if (item33.emp_num.Trim().ToUpper() == "CONT")
                                                            {
                                                                red.guid = 0;

                                                            }
                                                            else
                                                            {
                                                                red.guid = item1.RateAmount * newquatity;
                                                            }
                                                        }
                                                        if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                                        {
                                                            red.guid = item1.SettingMax;
                                                        }
                                                        mylistfinal.Add(red);
                                                    }
                                                    if (wwabsnum != item33.abs_num)
                                                    {
                                                        wwabsnum = item33.abs_num;
                                                    }
                                                }
                                            }


                                        }
                                    }


                                }
                                else
                                {
                                    if (inout == 2)
                                    {
                                        foreach (var mygroup in groupPonew2)
                                        {
                                            item1.RateAmount = wamm.RateAmount;
                                            var calresult = (from c in db.picks
                                                             where c.order_ == mygroup.Remotekey && c.order_suffix == mygroup.posuffix && c.co_num == conum
                                                             && c.wh_num == whnum
                                                             select c).GroupBy(x => x.lot).ToList();
                                            if (foundRatecal.CalculRate == 9)
                                            {
                                                calresult = (from c in db.picks
                                                             where c.order_ == mygroup.Remotekey && c.order_suffix == mygroup.posuffix && c.co_num == conum
                                                             && c.wh_num == whnum && string.IsNullOrEmpty(c.lot) == false
                                                             select c).GroupBy(x => x.lot).ToList();
                                            }
                                            AudiViewModel red = new AudiViewModel();
                                            red.wh_num = whnum;
                                            red.co_num = conum;
                                            red.substat_code = item1.RateDescId;
                                            red.oldunitcost = item1.SettingMin;
                                            red.transmission = item1.SettingId;
                                            red.unitcost = item1.RateAmount; red.action_code = item1.UnitCode;
                                            red.fromdatetime = item1.ChargeName; red.po_number = mygroup.Remotekey; red.po_suffix = mygroup.posuffix; red.abs_num = "";
                                            red.po_line = 0; red.lot = ""; red.row_status = ""; red.sugg_qty = calresult.Count;
                                            red.act_qty = 0; red.item_type = ""; red.emp_num = "0";
                                            red.result_msg = red.po_number + " " + red.po_suffix; red.case_qtyitem = 0; red.box_qtyitem = 0;
                                            itemqty = 0; red.cc_type = item1.UnitCode; var calresultt = calresult.Count;
                                            if (calresultt != 0)
                                            {
                                                if (item1.UnitCode.ToUpper() == "HR" || item1.UnitCode.ToUpper() == "HHR")
                                                {
                                                    if (foundsettingcustomer != null)
                                                    {
                                                        if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                        if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                        if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                        hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                        calresultt = (int)Getquantity("HR", hincrem, hourlymin, calresultt);
                                                    }
                                                }
                                            }

                                            red.extdcost = item1.RateAmount * calresultt;
                                            red.doc_id = foundNameabrg.NameAbrg;

                                            if (red.oldunitcost > red.extdcost)
                                            {
                                                red.guid = item1.SettingMin;
                                            }


                                            else
                                            {

                                                red.guid = item1.RateAmount * calresultt;


                                            }
                                            if (red.guid > item1.SettingMax && item1.SettingMax != 0)
                                            {
                                                red.guid = item1.SettingMax;
                                            }
                                            mylistfinal.Add(red);

                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                //*************************** End of wsetting


            }
            decimal? newcall = 0, gencall = 0;
            var ress1 = mylistfinal5.GroupBy(x => new { x.po_number, x.po_suffix, x.fromdatetime }).ToList();
            foreach (var itemline in ress1)
            {
                newcall = 0; gencall = 0;
                var ress = mylistfinal5.Where(x => x.po_number == itemline.Key.po_number && x.po_suffix == itemline.Key.po_suffix && x.fromdatetime == itemline.Key.fromdatetime).ToList();
                foreach (var itemress in ress)
                {
                    gencall = itemress.oldunitcost;
                    newcall += itemress.quantity * itemress.extdcost;

                }
                AudiViewModel red2 = new AudiViewModel();
                if (ress.Count != 0)
                {
                    red2.abs_num = "";
                    red2.act_qty = ress[0].act_qty;
                    red2.action_code = ress[0].action_code;
                    red2.adj_code = ress[0].adj_code;
                    red2.batch = ress[0].batch;
                    red2.bin_from = ress[0].bin_from;
                    red2.bin_num = ress[0].bin_num;
                    red2.bin_to = ress[0].bin_to;
                    red2.box_qtyitem = ress[0].box_qtyitem;
                    red2.cancelled = ress[0].cancelled;
                    red2.cancelled_at = ress[0].cancelled_at;
                    red2.cancelled_by = ress[0].cancelled_by;
                    red2.cargo_control = ress[0].cargo_control;
                    red2.carton_id = ress[0].carton_id;
                    red2.case_qty = ress[0].case_qty;
                    red2.case_qtyitem = ress[0].case_qtyitem;
                    red2.cc_string = ress[0].cc_string;
                    red2.transmission = ress[0].transmission;
                    red2.cc_type = ress[0].cc_type;
                    red2.co_num = ress[0].co_num;
                    red2.comments = ress[0].comments;
                    red2.country_code = ress[0].country_code;
                    red2.date_time = ress[0].date_time;
                    red2.date_time2 = ress[0].date_time2;
                    red2.doc_id = ress[0].doc_id;
                    red2.emp_num = ress[0].emp_num;
                    red2.extdcost = ress[0].extdcost;
                    red2.fromdatetime = ress[0].fromdatetime;
                    red2.guid = ress[0].guid;
                    red2.item_num = ress[0].item_num;
                    red2.item_qty = ress[0].item_qty;
                    red2.item_type = ress[0].item_type;
                    red2.line_sequence = ress[0].line_sequence;
                    red2.lot = ress[0].lot;
                    red2.substat_code = ress[0].substat_code;
                    red2.msg_status = ress[0].msg_status;
                    red2.old_stock_stat = ress[0].old_stock_stat;
                    red2.oldextdcost = ress[0].oldextdcost;
                    red2.oldunitcost = ress[0].oldunitcost;
                    red2.pallet_id = ress[0].pallet_id;
                    red2.pallet_id_from = ress[0].pallet_id_from;
                    red2.po_line = ress[0].po_line;
                    red2.po_number = ress[0].po_number;
                    red2.po_suffix = ress[0].po_suffix;
                    red2.pool = ress[0].pool;
                    red2.qa_release_id = red2.qa_release_id;
                    red2.action_code = ress[0].action_code;
                    red2.release_id = ress[0].release_id;
                    red2.result_code = ress[0].result_code;
                    red2.result_msg = ress[0].result_msg;
                    red2.row_status = ress[0].row_status;
                    red2.sugg_qty = ress[0].sugg_qty;
                    red2.todatetime = ress[0].todatetime;
                    red2.trans_type = ress[0].trans_type;
                    red2.unitcost = ress[0].unitcost;
                    red2.uom = ress[0].uom;
                    red2.wh_num = ress[0].wh_num;

                    red2.quantity = 1;
                    red2.guid = ress[0].guid;
                    if (newcall < gencall) red2.guid = gencall;

                    mylistfinal.Add(red2);
                }
            }
            if (mylistfinal.Count != 0)
            {
                mylist = mylistfinal;
            }

            var groupPo = (from c in mylist
                           where c.po_number != ""
                           group c by new { c.po_number, c.po_suffix } into d
                           select new
                           {
                               Remotekey = d.Key.po_number,
                               posuffix = d.Key.po_suffix,
                               Total = d.Distinct().Count(),
                               TotalSum = d.Sum(x => x.guid)

                           }).ToList();


            //************************************************************


            var rt = db1.SettingRates.Join(db1.RateDescs,
                                           so => new { so.RateDescId },
                                         to => new { to.RateDescId },
                                         (so, to) => new { so, to })
                                     .Join(db1.Ratebys,
                                         so5 => new { so5.so.RatebyId },
                                         to5 => new { to5.RatebyId },
                                         (so5, to5) => new { so5, to5 })

                                         .Where(x => x.so5.so.co_num == conum && x.so5.so.wh_num == whnum && x.so5.to.RateDescCountPo == 1 && x.so5.to.RatebyQty == 0 && x.so5.so.KindId == inout)
                                         .Select(z => new { z.so5.so, z.so5.to, z.to5 }).ToList();
            var forflatout = 0;
            decimal? wamount;
            foreach (var item in rt)
            {
                forflatout = 0;
                db1.Dispose(); db1 = new RateContext();
                var wamm = db1.SettingRates.FirstOrDefault(x => x.SettingId == item.so.SettingId);
                item.so.RateAmount = wamm.RateAmount;
                wamount = item.so.RateAmount;
                var customersetting = db7.GetAll().FirstOrDefault(x => x.SettingBaseWhnum == item.so.wh_num && x.SettingBaseConum == item.so.co_num);
                 var foundNameabrg = db1.RateDescs.Where(x => x.RateDescId == item.so.RateDescId).FirstOrDefault();
                if (item.to5.CalculRate == 310 && inout == 2)
                {

                    var result10checkinv = db.inventories.Join(
                                               db.items,

                                               so90 => new { so90.co_num, so90.wh_num, so90.abs_num },
                                               to90 => new { to90.co_num, to90.wh_num, to90.abs_num },
                                               (so90, to90) => new { so90, to90 })

                      .Where(z => z.so90.co_num == conum && z.so90.wh_num == whnum && z.to90.hazardous == "T")
                      .GroupBy(x => x.so90.abs_num)
                      .Select(g => new
                      {
                          absnum = g.Sum(s => s.so90.total_qty)
                      }).ToList();
                    if (result10checkinv != null)
                    {
                        item.so.RateAmount = wamount;
                        AudiViewModel red = new AudiViewModel();
                        red.substat_code = item.so.RateDescId;
                        red.wh_num = whnum;
                        red.co_num = conum;
                        red.quantity = result10checkinv.Count;
                        red.substat_code = item.so.RateDescId;
                        red.action_code = item.so.UnitCode;
                        red.transmission = item.so.SettingId;
                        red.oldunitcost = item.so.SettingMin;
                        red.unitcost = item.so.RateAmount;
                        red.fromdatetime = item.so.ChargeName;
                        red.po_number = "";
                        red.po_suffix = "";
                        if (item.so.UnitCode.ToUpper() == "HR" || item.so.UnitCode.ToUpper() == "HHR")
                        {
                            if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                            if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                            if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                            hourlymin = foundsettingcustomer.SettingBaseNew3;
                            red.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red.quantity);
                        }
                        red.extdcost = item.so.RateAmount * red.quantity;
                        red.sugg_qty = 0;
                        red.guid = item.so.RateAmount * red.quantity;
                        if (item.so.SettingMin > 0)
                        {
                            if (red.guid < item.so.SettingMin && red.quantity != 0) red.guid = item.so.SettingMin;
                        }
                        if (item.so.SettingMax > 0)
                        {
                            if (red.guid > item.so.SettingMax && red.quantity != 0) red.guid = item.so.SettingMax;
                        }

                        red.case_qtyitem = 0;
                        red.box_qtyitem = 0;
                        red.result_msg = "";

                        red.emp_num = "";
                        if (red.oldunitcost > red.extdcost)
                        {
                            red.guid = item.so.SettingMin;
                        }
                        if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                        {
                            red.guid = item.so.SettingMax;
                        }
                        mylistfinal.Add(red);
                    }


                }
                else if (item.to5.CalculRate == 200)
                {

                    foreach (var mygroup in groupPonew88)
                    {
                        AudiViewModel red = new AudiViewModel();
                        red.substat_code = item.so.RateDescId;
                        red.wh_num = whnum;
                        red.co_num = conum;
                        red.oldunitcost = item.so.SettingMin;
                        red.transmission = item.so.SettingId;
                        red.action_code = item.so.UnitCode;
                        red.unitcost = item.so.RateAmount; red.quantity = 1;
                        red.fromdatetime = item.so.ChargeName; red.po_number = mygroup.Remotekey; red.po_suffix = mygroup.posuffix; red.abs_num = "";
                        red.po_line = 0; red.lot = ""; red.row_status = ""; red.sugg_qty = 1;
                        red.act_qty = 0; red.item_type = ""; red.emp_num = "0";
                        red.result_msg = ""; red.case_qtyitem = 0; red.box_qtyitem = 0;
                        itemqty = 0;
                        red.extdcost = item.so.RateAmount * 1;
                        red.doc_id = foundNameabrg.NameAbrg;
                        red.guid = item.so.RateAmount * 1;
                        red.cc_type = item.so.UnitCode;

                        if (item.so.SettingMin > red.guid)
                        {


                            red.guid = item.so.SettingMin;

                        }
                        if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                        {
                            red.guid = item.so.SettingMax;
                        }
                        if (item.to.TypeCalculate == 8 || item.to.TypeCalculate == 9 || item.to.TypeCalculate == 10)
                        {
                            var parceusps = "";
                            if (item.to.RateDescName.ToUpper() == "Parcel UPS".ToUpper()) parceusps = "UPS";
                            if (item.to.RateDescName.ToUpper() == "Parcel Fedex".ToUpper()) parceusps = "FEPL";
                            if (item.to.RateDescName.ToUpper() == "Parcel USPS".ToUpper()) parceusps = "USPS";
                            var rres = (from c in db.ordhdrs
                                        where c.order_ == mygroup.Remotekey && c.order_suffix == mygroup.posuffix
                                            && c.co_num == conum && c.wh_num == whnum && c.carrier == parceusps
                                        select c).ToList();
                            if (rres.Count == 0)
                            {
                                red.guid = 0;
                                red.extdcost = 0;
                                red.quantity = 0;
                            }
                        }
                        mylistfinal.Add(red);
                    }
                }
              
                else
                {
                    if (item.to5.CalculRate != 31)
                    {
                        var todatte = DateTime.Now.AddDays(-1);
                        var todatestoragee = new DateTime(todatte.Year, todatte.Month, todatte.Day, 23, 59, 59);
                       
                        else
                        {
                            if (item.to5.CalculRate != 31)
                            {
                                if (item.to.RateDescCountPo == 1)
                                {
                                    
                                    
                                    {
                                        decimal? ammmount = 0; int fgh = 0;
                                        for (int ib = 0; ib < groupPo.Count(); ib++)
                                        {
                                            item.so.RateAmount = wamount;
                                            decimal? wfreight = 0;
                                            string vb = groupPo[ib].Remotekey.Trim().ToUpper();
                                            var myquery = db10.GetAll().Where(x => x.UserText2 == conum && x.UserText3 == whnum && x.InvoiceNumber.Trim().ToUpper() == vb).ToList();
                                            foreach (var queryitem in myquery)
                                            {
                                                if (queryitem.VoidFlag == "N") wfreight += queryitem.UserNumber1;
                                            }

                                            forflatout += 1;
                                            AudiViewModel red = new AudiViewModel();
                                            if (inout == 3) red.doc_id = "S";
                                            red.substat_code = item.so.RateDescId;
                                            red.wh_num = whnum;
                                            red.co_num = conum;
                                            red.substat_code = item.so.RateDescId;
                                            red.action_code = item.so.UnitCode;
                                            red.transmission = item.so.SettingId;
                                            red.oldunitcost = item.so.SettingMin;
                                            red.unitcost = item.so.RateAmount;
                                            red.fromdatetime = item.so.ChargeName;
                                            red.po_number = groupPo[ib].Remotekey;
                                            red.po_suffix = groupPo[ib].posuffix;
                                            red.quantity = 0;

                                            
                                            
                                            {
                                                if (item.to5.CalculRate == 8 && inout == 2)
                                                {
                                                    var calresult = (from c in db.orddtls
                                                                     join k in db.ordhdrs on c.id equals k.id
                                                                     where k.order_ == red.po_number && k.order_suffix == red.po_suffix && k.co_num == conum
                                                                     select c).ToList();
                                                    red.quantity = calresult.Count();
                                                }
                                                else if (item.to5.CalculRate == 44)
                                                {
                                                    red.po_number = "";
                                                    red.po_suffix = "";
                                                    if (forflatout == 1)
                                                    {
                                                        red.quantity = 1;
                                                    }
                                                    else
                                                    {
                                                        red.quantity = 0;
                                                    }
                                                }
                                                else
                                                {
                                                    if (item.to5.CalculRate == 251 && inout == 2)
                                                    {
                                                        var calresult = db.ordhdrs.FirstOrDefault(k => k.order_ == red.po_number && k.order_suffix == red.po_suffix && k.co_num == conum
                                                                               && k.wh_num == whnum);
                                                        if (calresult.carrier.ToUpper() == "USPS")
                                                        {
                                                            red.quantity = wfreight;
                                                            item.so.RateAmount += 1;
                                                            fgh = 1;
                                                            if (myquery == null)
                                                            {
                                                                foreach (var itemquery in myquery)
                                                                {

                                                                    red.quantity = 0;
                                                                    item.so.RateAmount -= 1;
                                                                    fgh = 0;
                                                                }

                                                            }
                                                            if (myquery != null)
                                                            {
                                                                foreach (var itemquery in myquery)
                                                                {
                                                                    if (itemquery.Shipper.ToUpper() != "AC01")
                                                                    {
                                                                        red.quantity = 0;
                                                                        item.so.RateAmount -= 1;
                                                                        fgh = 0;
                                                                    }
                                                                }

                                                            }
                                                        }

                                                        else
                                                        {

                                                            if (item.to5.CalculRate == 30)
                                                            {
                                                                red.quantity = 0;
                                                            }
                                                            else
                                                            {
                                                                red.quantity = 0;
                                                            }
                                                        }

                                                    }
                                                    else
                                                    {
                                                        red.quantity = 0;
                                                    }
                                                    

                                                }

                                            }
                                            if (item.to5.CalculRate == 251 && inout == 2)
                                            {
                                                item.so.RateAmount = 1;
                                                if (myquery != null)
                                                {
                                                    foreach (var itemquery in myquery)
                                                    {

                                                        if (itemquery.Shipper.ToUpper() != "AC01")
                                                        {
                                                            red.quantity = 0;
                                                        }
                                                    }
                                                }


                                                ammmount = item.so.RateAmount * red.quantity;
                                            }
                                            
                                           
                                            red.extdcost = item.so.RateAmount * red.quantity;
                                            red.sugg_qty = 0;
                                            red.guid = item.so.RateAmount * red.quantity;
                                            if (item.to5.CalculRate != 251)
                                            {
                                                if (item.so.SettingMin > 0)
                                                {
                                                    if (red.guid < item.so.SettingMin && red.quantity != 0) red.guid = item.so.SettingMin;
                                                }
                                                if (item.so.SettingMax > 0)
                                                {
                                                    if (red.guid > item.so.SettingMax && red.quantity != 0) red.guid = item.so.SettingMax;
                                                }
                                            }
                                            totalgeneral += red.guid;
                                            red.case_qtyitem = 0;
                                            red.box_qtyitem = 0;
                                            red.result_msg = groupPo[ib].Remotekey + "-" + groupPo[ib].posuffix;

                                            red.emp_num = red.emp_num;
                                            if (red.oldunitcost > red.extdcost)
                                            {
                                                red.guid = item.so.SettingMin;
                                            }
                                            if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                                            {
                                                red.guid = item.so.SettingMax;
                                            }
                                            if (inout == 2)
                                            {
                                                if (item.to.NameAbrg == "H")
                                                {
                                                    red.doc_id = "H";
                                                }
                                            }
                                            var ponumber = groupPo[ib].Remotekey; var suffixnumber = groupPo[ib].posuffix;
                                            if (item.to5.CalculRate == 80 && inout == 2 && item.to.TypeCalculate == 24)
                                            {
                                                red.abs_num = "";
                                                var rres = (from c in db.ordhdrs
                                                            where c.order_ == ponumber && c.order_suffix == suffixnumber
                                                                && c.co_num == conum && c.wh_num == whnum
                                                            select c).ToList();
                                                var finde = rres.Where(x => x.type == "E").ToList();
                                                if (finde.Count == 0)
                                                {
                                                    red.guid = 0;
                                                    red.extdcost = 0;
                                                    red.quantity = 0;
                                                }
                                                else
                                                {
                                                    red.quantity = 1;
                                                    red.guid = item.so.RateAmount * red.quantity;
                                                    red.extdcost = item.so.RateAmount;

                                                }
                                            }
                                            if (item.to5.CalculRate == 70 && inout == 2)
                                            {
                                                var rres = (from c in db.ordhdrs
                                                            where c.order_ == ponumber && c.order_suffix == suffixnumber
                                                                && c.co_num == conum && c.wh_num == whnum
                                                            select c).ToList();
                                                var rees1 = rres.FirstOrDefault();
                                                if (rees1.carrier.ToUpper() != "CUST" && rees1.carrier.ToUpper() != "WILL")
                                                {
                                                    red.guid = 0;
                                                    red.extdcost = 0;
                                                    red.quantity = 0;
                                                }
                                                if (rees1.carrier.ToUpper() == "CUST" || rees1.carrier.ToUpper() == "WILL")
                                                {
                                                    var rre1s = (from c in db.ordhdrs
                                                                 where c.order_ == ponumber && c.order_suffix == suffixnumber
                                                                     && c.co_num == conum && c.wh_num == whnum
                                                                 select c).ToList();
                                                    red.quantity = 1;
                                                    red.extdcost = wamount * red.quantity;
                                                    red.sugg_qty = 0;
                                                    red.guid = wamount * red.quantity;

                                                    if (item.so.SettingMin > 0)
                                                    {
                                                        if (red.guid < item.so.SettingMin && red.quantity != 0) red.guid = item.so.SettingMin;
                                                    }
                                                    if (item.so.SettingMax > 0)
                                                    {
                                                        if (red.guid > item.so.SettingMax && red.quantity != 0) red.guid = item.so.SettingMax;
                                                    }

                                                }
                                            }
                                            if (item.to.TypeCalculate == 8 && inout == 2)
                                            {
                                                if (item.to5.CalculRate != 42)
                                                {
                                                    var rres = (from c in db.ordhdrs
                                                                where c.order_ == ponumber && c.order_suffix == suffixnumber
                                                                    && c.co_num == conum && c.wh_num == whnum
                                                                select c).ToList();
                                                    var rees1 = rres.FirstOrDefault();
                                                    if (rees1.carrier.ToUpper() != "UPS")
                                                    {
                                                        red.guid = 0;
                                                        red.extdcost = 0;
                                                        red.quantity = 0;
                                                    }
                                                }
                                                else
                                                {
                                                    var cartomst = (from cin in db.cartonmsts
                                                                    where (cin.order_ == ponumber) && (cin.order_suffix == suffixnumber) && (cin.co_num == conum) && (cin.wh_num == whnum)
                                                                    select cin).ToList();
                                                    var rres = (from c in db.ordhdrs
                                                                where c.order_ == ponumber && c.order_suffix == suffixnumber
                                                                    && c.co_num == conum && c.wh_num == whnum
                                                                select c).FirstOrDefault();
                                                    var rescstm1 = 0;
                                                    foreach (var itemhistory in cartomst)
                                                    {
                                                        try
                                                        {
                                                            var rescstm = db10.GetAll().Where(x => x.PackageID == itemhistory.carton_id && x.VoidFlag == "N").ToList();


                                                            if (rres.carrier.ToUpper() != "UPS")
                                                            {

                                                            }
                                                            else
                                                            {
                                                                rescstm1 = rescstm.Distinct().Count();


                                                            }
                                                        }
                                                        catch (Exception)
                                                        {


                                                        }

                                                    }
                                                    red.extdcost = item.so.RateAmount; red.quantity = rescstm1;
                                                    red.guid = item.so.RateAmount * red.quantity;
                                                    if (item.so.SettingMin > 0)
                                                    {
                                                        if (red.guid < item.so.SettingMin) red.guid = item.so.SettingMin;
                                                    }
                                                    if (item.so.SettingMax > 0)
                                                    {
                                                        if (red.guid > item.so.SettingMax) red.guid = item.so.SettingMax;
                                                    }
                                                }
                                            }
                                            if (item.to.TypeCalculate == 9 && inout == 2)
                                            {
                                                if (item.to5.CalculRate != 42)
                                                {
                                                    var rres = (from c in db.ordhdrs
                                                                where c.order_ == ponumber && c.order_suffix == suffixnumber
                                                                    && c.co_num == conum && c.wh_num == whnum
                                                                select c).ToList();
                                                    var rees1 = rres.FirstOrDefault();
                                                    if (rees1.carrier.ToUpper() != "FEPL")
                                                    {
                                                        red.guid = 0;
                                                        red.extdcost = 0;
                                                        red.quantity = 0;
                                                    }
                                                }
                                                else
                                                {
                                                    var cartomst = (from cin in db.cartonmsts
                                                                    where (cin.order_ == ponumber) && (cin.order_suffix == suffixnumber) && (cin.co_num == conum) && (cin.wh_num == whnum)
                                                                    select cin).ToList();
                                                    var rres = (from c in db.ordhdrs
                                                                where c.order_ == ponumber && c.order_suffix == suffixnumber
                                                                    && c.co_num == conum && c.wh_num == whnum
                                                                select c).FirstOrDefault();
                                                    var rescstm1 = 0;
                                                    foreach (var itemhistory in cartomst)
                                                    {
                                                        try
                                                        {
                                                            var rescstm = db10.GetAll().Where(x => x.PackageID == itemhistory.carton_id && x.VoidFlag == "N").ToList();


                                                            if (rres.carrier.ToUpper() != "FEPL")
                                                            {

                                                            }
                                                            else
                                                            {
                                                                rescstm1 = rescstm.Distinct().Count();


                                                            }
                                                        }
                                                        catch (Exception)
                                                        {


                                                        }

                                                    }
                                                    red.extdcost = item.so.RateAmount; red.quantity = rescstm1;
                                                    red.guid = item.so.RateAmount * red.quantity;
                                                    if (item.so.SettingMin > 0)
                                                    {
                                                        if (red.guid < item.so.SettingMin) red.guid = item.so.SettingMin;
                                                    }
                                                    if (item.so.SettingMax > 0)
                                                    {
                                                        if (red.guid > item.so.SettingMax) red.guid = item.so.SettingMax;
                                                    }
                                                }
                                            }
                                            if (item.to.TypeCalculate == 10 && inout == 2)
                                            {
                                                if (item.to5.CalculRate != 42)
                                                {
                                                    var rres = (from c in db.ordhdrs
                                                                where c.order_ == ponumber && c.order_suffix == suffixnumber
                                                                    && c.co_num == conum && c.wh_num == whnum
                                                                select c).ToList();
                                                    var rees1 = rres.FirstOrDefault();
                                                    if (rees1.carrier.ToUpper() != "USPS")
                                                    {
                                                        red.guid = 0;
                                                        red.extdcost = 0;
                                                        red.quantity = 0;
                                                    }
                                                }
                                                else
                                                {
                                                    var cartomst = (from cin in db.cartonmsts
                                                                    where (cin.order_ == ponumber) && (cin.order_suffix == suffixnumber) && (cin.co_num == conum) && (cin.wh_num == whnum)
                                                                    select cin).ToList();
                                                    var rres = (from c in db.ordhdrs
                                                                where c.order_ == ponumber && c.order_suffix == suffixnumber
                                                                    && c.co_num == conum && c.wh_num == whnum
                                                                select c).FirstOrDefault();
                                                    var rescstm1 = 0;
                                                    foreach (var itemhistory in cartomst)
                                                    {
                                                        try
                                                        {
                                                            var rescstm = db10.GetAll().Where(x => x.PackageID == itemhistory.carton_id && x.VoidFlag == "N").ToList();


                                                            if (rres.carrier.ToUpper() != "USPS")
                                                            {

                                                            }
                                                            else
                                                            {
                                                                rescstm1 = rescstm.Distinct().Count();


                                                            }
                                                        }
                                                        catch (Exception)
                                                        {


                                                        }

                                                    }
                                                    red.extdcost = item.so.RateAmount; red.quantity = rescstm1;
                                                    red.guid = item.so.RateAmount * red.quantity;
                                                    if (item.so.SettingMin > 0)
                                                    {
                                                        if (red.guid < item.so.SettingMin) red.guid = item.so.SettingMin;
                                                    }
                                                    if (item.so.SettingMax > 0)
                                                    {
                                                        if (red.guid > item.so.SettingMax) red.guid = item.so.SettingMax;
                                                    }
                                                }
                                            }
                                            if (item.to.TypeCalculate == 12 && inout == 2)
                                            {
                                                if (item.to5.CalculRate == 42)
                                                {

                                                    var rres = (from c in db.ordhdrs
                                                                where c.order_ == ponumber && c.order_suffix == suffixnumber
                                                                    && c.co_num == conum && c.wh_num == whnum
                                                                select c).FirstOrDefault();
                                                    try
                                                    {
                                                        if (rres.carrier.ToUpper() != "USP" || rres.carrier.ToUpper() != "FEPL" || rres.carrier.ToUpper() != "USPS")
                                                        {
                                                            red.guid = 0;
                                                            red.extdcost = 0;
                                                            red.quantity = 0;
                                                        }
                                                        else
                                                        {
                                                            if (rres.ship_country.ToUpper() != "US" || rres.ship_country.ToUpper() != "USA")
                                                            {
                                                                red.extdcost = item.so.RateAmount; red.quantity = 1;
                                                                red.guid = item.so.RateAmount * red.quantity;
                                                                if (item.so.SettingMin > 0)
                                                                {
                                                                    if (red.guid < item.so.SettingMin) red.guid = item.so.SettingMin;
                                                                }
                                                                if (item.so.SettingMax > 0)
                                                                {
                                                                    if (red.guid > item.so.SettingMax) red.guid = item.so.SettingMax;
                                                                }


                                                            }
                                                            else
                                                            {
                                                                red.guid = 0;
                                                                red.extdcost = 0;
                                                                red.quantity = 0;
                                                            }


                                                        }
                                                    }
                                                    catch (Exception)
                                                    {

                                                        red.guid = 0;
                                                        red.extdcost = 0;
                                                        red.quantity = 0;
                                                    }

                                                }
                                            }
                                            if (item.to.TypeCalculate == 21 && inout == 2)
                                            {
                                                if (item.to5.CalculRate == 252)
                                                {


                                                    var itemabs = db.items.FirstOrDefault(x => x.abs_num.ToUpper() == item.so.abs_num.ToUpper() && x.co_num == item.so.co_num && x.wh_num == item.so.wh_num);
                                                    if (itemabs.hazardous == "T")
                                                    {
                                                        red.extdcost = item.so.RateAmount; red.quantity = 1;
                                                        red.guid = item.so.RateAmount * red.quantity;
                                                        if (item.so.SettingMin > 0)
                                                        {
                                                            if (red.guid < item.so.SettingMin) red.guid = item.so.SettingMin;
                                                        }
                                                        if (item.so.SettingMax > 0)
                                                        {
                                                            if (red.guid > item.so.SettingMax) red.guid = item.so.SettingMax;
                                                        }
                                                    }
                                                    else
                                                    {
                                                        red.guid = 0;
                                                        red.extdcost = 0;
                                                        red.quantity = 0;
                                                    }


                                                }
                                            }
                                            if (item.to.TypeCalculate == 22 && inout == 2)
                                            {
                                                if (item.to5.CalculRate == 253)
                                                {
                                                    var aud = db.auditlogs.Where(x => x.po_number == ponumber && x.po_suffix == suffixnumber && x.co_num == conum && x.wh_num == whnum && x.trans_type == "OE").ToList();

                                                    if (aud.Count != 0)
                                                    {
                                                        red.extdcost = item.so.RateAmount; red.quantity = 1;
                                                        red.guid = item.so.RateAmount * red.quantity;
                                                        if (item.so.SettingMin > 0)
                                                        {
                                                            if (red.guid < item.so.SettingMin) red.guid = item.so.SettingMin;
                                                        }
                                                        if (item.so.SettingMax > 0)
                                                        {
                                                            if (red.guid > item.so.SettingMax) red.guid = item.so.SettingMax;
                                                        }
                                                    }
                                                    else
                                                    {
                                                        red.guid = 0;
                                                        red.extdcost = 0;
                                                        red.quantity = 0;
                                                    }


                                                }
                                            }
                                            if (item.to5.CalculRate == 30)
                                            {
                                                var findco = (from c in db1.PoCloseRequireds
                                                              where c.Poconum == conum && c.Powhnum == whnum
                                                              && c.Poponumber == ponumber && c.Posuffix == suffixnumber
                                                              && c.PoCloseDescId == item.to.RateDescId
                                                              select c).FirstOrDefault();
                                                try
                                                {
                                                    red.guid = item.so.RateAmount * findco.PoCloseRate;
                                                    if (item.so.SettingMin > 0)
                                                    {
                                                        if (red.guid < item.so.SettingMin) red.guid = item.so.SettingMin;
                                                    }
                                                    if (item.so.SettingMax > 0)
                                                    {
                                                        if (red.guid > item.so.SettingMax) red.guid = item.so.SettingMax;
                                                    }
                                                    red.quantity = findco.PoCloseRate;
                                                }
                                                catch (Exception)
                                                {
                                                    red.sugg_qty = 0;
                                                    red.oldunitcost = 0;
                                                    red.guid = 0;
                                                    red.extdcost = 0;
                                                    red.quantity = 0;
                                                }
                                            }
                                            if (item.to5.CalculRate == 251 && inout == 2)
                                            {
                                                if (fgh == 1)
                                                {

                                                    item.so.RateAmount = wamm.RateAmount;
                                                    item.so.RateAmount += 1;
                                                    red.unitcost = 1;
                                                    if (myquery != null)
                                                    {
                                                        foreach (var itemquery in myquery)
                                                        {

                                                            if (itemquery.Shipper.ToUpper() != "AC01")
                                                            {
                                                                item.so.RateAmount = 0;
                                                                red.unitcost = 0;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if (item.to5.CalculRate == 251 && inout == 2)
                                            {
                                                if (fgh == 1)
                                                {
                                                    var myamount = red.unitcost;
                                                    red.unitcost = red.quantity;
                                                    red.quantity = 1;
                                                    item.so.RateAmount = 1;
                                                    if (myquery != null)
                                                    {
                                                        foreach (var itemquery in myquery)
                                                        {
                                                            if (itemquery.Shipper.ToUpper() != "AC01")
                                                            {
                                                                item.so.RateAmount = 0;
                                                                red.unitcost = 0;
                                                                red.quantity = 0;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            mylistfinal.Add(red); decimal? wqunt = 0;
                                            if (item.to5.CalculRate == 251 && inout == 2)
                                            {
                                                if (fgh == 1)
                                                {
                                                    //var myamount = red.quantity;
                                                    wqunt = red.unitcost;
                                                    //item.so.RateAmount = myamount;
                                                }
                                            }
                                            if (item.to5.CalculRate == 251 && inout == 2 && fgh == 1)
                                            {
                                                item.so.RateAmount = wamm.RateAmount;
                                                AudiViewModel red67 = new AudiViewModel();
                                                if (inout == 3) red67.doc_id = "S";
                                                red67.substat_code = item.so.RateDescId;
                                                red67.wh_num = whnum;
                                                red67.co_num = conum;
                                                red67.substat_code = item.so.RateDescId;
                                                red67.action_code = item.so.UnitCode;
                                                red67.transmission = item.so.SettingId;
                                                red67.oldunitcost = item.so.SettingMin;
                                                red67.unitcost = item.so.RateAmount;
                                                red67.fromdatetime = "61";
                                                red67.quantity = wqunt;
                                                red67.po_number = groupPo[ib].Remotekey;
                                                red67.po_suffix = groupPo[ib].posuffix;
                                                red67.extdcost = item.so.RateAmount * red67.quantity;
                                                red67.sugg_qty = 0;
                                                ammmount = ammmount + (item.so.RateAmount * red67.quantity);
                                                red67.guid = item.so.RateAmount * red67.quantity;
                                                if (item.so.SettingMin > 0)
                                                {
                                                    if (ammmount < item.so.SettingMin && red67.quantity != 0) red67.guid = item.so.SettingMin;
                                                }
                                                if (item.so.SettingMax > 0)
                                                {
                                                    if (ammmount > item.so.SettingMax && red67.quantity != 0) red67.guid = item.so.SettingMax;
                                                }
                                                totalgeneral += red67.guid;
                                                red67.case_qtyitem = 0;
                                                red67.box_qtyitem = 0;
                                                red67.result_msg = groupPo[ib].Remotekey + "-" + groupPo[ib].posuffix;

                                                red67.emp_num = red67.emp_num;
                                                if (red67.oldunitcost > ammmount)
                                                {
                                                    red67.guid = item.so.SettingMin;
                                                }
                                                if (ammmount > item.so.SettingMax && item.so.SettingMax != 0)
                                                {
                                                    red67.guid = item.so.SettingMax;
                                                }
                                                mylistfinal.Add(red67);
                                            }
                                            // }
                                        }
                                    }
                                }
                                else
                                {
                                    item.so.RateAmount = wamount;
                                    AudiViewModel red = new AudiViewModel();
                                    red.substat_code = item.so.RateDescId;
                                    red.wh_num = whnum;
                                    red.co_num = conum;
                                    red.oldunitcost = item.so.SettingMin;
                                    red.action_code = item.so.UnitCode;
                                    red.unitcost = item.so.RateAmount;
                                    red.transmission = item.so.SettingId;
                                    red.fromdatetime = item.so.ChargeName;
                                    red.po_number = "";
                                    red.extdcost = item.so.RateAmount;
                                    red.quantity = 1;
                                    red.sugg_qty = 0;
                                    red.emp_num = red.emp_num;
                                    red.guid = item.so.RateAmount;
                                    if (item.so.SettingMin > 0)
                                    {
                                        if (red.guid < item.so.SettingMin) red.guid = item.so.SettingMin;
                                    }
                                    if (item.so.SettingMax > 0)
                                    {
                                        if (red.guid > item.so.SettingMax) red.guid = item.so.SettingMax;
                                    }
                                    totalgeneral += red.guid;
                                    red.case_qtyitem = 0;
                                    red.box_qtyitem = 0;
                                    red.result_msg = red.result_msg;
                                    red.cc_type = item.so.UnitCode;
                                    mylistfinal.Add(red);
                                }
                            }
                        }
                    }
                }
            }
          

            if (mylistfinal.Count != 0)
            {
                mylist = mylistfinal;
            }
            mylistfinal2 = mylist;

            
            try
            {
                db1.Dispose(); db1 = new RateContext();
            }
            catch (Exception)
            {


            }

            //**************************************************************** By Po_number ratebyqty Storage
            decimal? newquantity22 = 0;
            decimal? newtotal = 0;
            ArrayList palletidlist = new ArrayList();
            var rt1 = db1.SettingRates.Join(db1.RateDescs,
                                           so => new { so.RateDescId },
                                         to => new { to.RateDescId },
                                         (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.RateDescCountPo == 1 && x.to.RatebyQty == 1 && x.so.KindId == inout)
                                        .Select(z => new { z.so, z.to }).ToList();

            DateTime dt100 = fromdatestorage1.AddDays(-1);
            var firstDayOfMonth00 = new DateTime(dt100.Year, dt100.Month, 1);
            var lastDayOfMonth00 = new DateTime(dt100.Year, dt100.Month, 1).AddMonths(1).AddDays(-1);
            foreach (var item44 in rt1)
            {
                var forflat = 0; var teststorage = 0;
                db1.Dispose(); db1 = new RateContext();
                var customersetting = db7.GetAll().Where(x => x.SettingBaseWhnum == whnum && x.SettingBaseConum == conum).ToList();
                var resull = customersetting.FirstOrDefault();

                int splitt = 0;
                if (resull != null)
                {
                    splitt = resull.SettingBaseSplitMinth;
                }
                if (item44.to.TypeCalculate == 39 || item44.to.TypeCalculate == 50 || item44.to.TypeCalculate == 51) teststorage = 1;
                var wamm = db1.SettingRates.FirstOrDefault(x => x.SettingId == item44.so.SettingId);
                item44.so.RateAmount = wamm.RateAmount;
                var search = db1.Ratebys.FirstOrDefault(x => x.RatebyId == item44.so.RatebyId);
                if (search.CalculRate != 31)
                {
                    
                    
                    {


                        
                        
                        {
                            decimal? totalqtyforitem = 0;
                            if (item44.to.TypeCalculate != 26)
                            {
                                var lastmylist2 = mylistfinal2.Where(x => x.quantity != 0 && x.abs_num != null && x.doc_id != "S" && x.bin_to == "x").GroupBy(x => new { x.po_number, x.po_suffix, x.abs_num, x.emp_num }).Select(x => new
                                {
                                    Remotekey = x.Key.po_number,
                                    posuffix = x.Key.po_suffix,
                                    absnum = x.Key.abs_num,
                                    empnum = x.Key.emp_num,
                                    Total = x.Sum(xx => xx.quantity)

                                }).OrderBy(x => x.Remotekey).ToList();

                                if (search.CalculRate != 31)
                                {
                                    if (search.CalculRate == 12)
                                    {
                                        lastmylist2 = mylistfinal2.Where(x => x.quantity != 0 && x.abs_num != null && x.doc_id != "S").GroupBy(x => new { x.po_number, x.po_suffix, x.abs_num, x.emp_num, x.bin_num }).Select(x => new
                                        {
                                            Remotekey = x.Key.po_number,
                                            posuffix = x.Key.po_suffix,
                                            absnum = x.Key.abs_num,
                                            empnum = x.Key.emp_num,
                                            Total = x.Sum(xx => xx.quantity)

                                        }).OrderBy(x => x.Remotekey).ToList();
                                    }
                                    
                                    
                                    {

                                        palletidlist.Clear();
                                        
                                        
                                        {
                                            var foundNameabrg = db1.RateDescs.Where(x => x.RateDescId == item44.so.RateDescId).FirstOrDefault();
                                            if (inout == 2 && search.CalculRate == 42)
                                            {
                                                var calresult = mylistfinal2.Where(x => x.quantity != 0 && x.abs_num != null && x.doc_id != "S" && x.bin_to == "x").GroupBy(x => new { x.po_number, x.po_suffix, x.abs_num, x.emp_num }).Select(x => new
                                                {
                                                    Remotekey = x.Key.po_number,
                                                    posuffix = x.Key.po_suffix,
                                                    absnum = x.Key.abs_num,

                                                    Total = x.Sum(xx => xx.quantity)

                                                }).OrderBy(x => x.Remotekey).ToList();
                                                var ponumberfor = ""; var suffixfor = ""; var dss = 0; var newcount = 0; decimal? amountofcal = 0;
                                                foreach (var itemcal in calresult)
                                                {
                                                    if (dss == 0)
                                                    {
                                                        dss = 1; ponumberfor = itemcal.Remotekey; suffixfor = itemcal.posuffix;
                                                    }
                                                    if (ponumberfor != itemcal.Remotekey || suffixfor != itemcal.posuffix)
                                                    {

                                                        if (newcount > 1)
                                                        {
                                                            if (amountofcal > item44.so.SettingMax && item44.so.SettingMax != 0)
                                                            {
                                                                AudiViewModel redd = new AudiViewModel();
                                                                redd.substat_code = item44.so.RateDescId;
                                                                redd.wh_num = whnum;
                                                                redd.co_num = conum; redd.abs_num = "";
                                                                redd.transmission = item44.so.SettingId;
                                                                redd.oldunitcost = item44.so.SettingMin;
                                                                redd.unitcost = item44.so.RateAmount;
                                                                redd.fromdatetime = item44.so.ChargeName; redd.po_number = ponumberfor; redd.po_suffix = suffixfor; redd.abs_num = "";
                                                                redd.po_line = 0; redd.lot = ""; redd.row_status = ""; redd.sugg_qty = 1;
                                                                redd.act_qty = 0; redd.item_type = ""; redd.emp_num = "0"; redd.quantity = 1;
                                                                redd.result_msg = redd.po_number + " " + redd.po_suffix; redd.case_qtyitem = 0; redd.box_qtyitem = 0;
                                                                itemqty = 0; redd.cc_type = item44.so.UnitCode;
                                                                redd.extdcost = amountofcal - item44.so.SettingMax;
                                                                redd.doc_id = foundNameabrg.NameAbrg; redd.guid = amountofcal - item44.so.SettingMax;
                                                                mylistfinal.Add(redd);
                                                            }
                                                            if (amountofcal < item44.so.SettingMin && item44.so.SettingMin != 0)
                                                            {
                                                                AudiViewModel red22 = new AudiViewModel();
                                                                red22.substat_code = item44.so.RateDescId;
                                                                red22.wh_num = whnum;
                                                                red22.co_num = conum; red22.abs_num = "";
                                                                red22.transmission = item44.so.SettingId;
                                                                red22.oldunitcost = item44.so.SettingMin;
                                                                red22.unitcost = item44.so.RateAmount;
                                                                red22.fromdatetime = item44.so.ChargeName; red22.po_number = ponumberfor; red22.po_suffix = suffixfor; red22.abs_num = "";
                                                                red22.po_line = 0; red22.lot = ""; red22.row_status = ""; red22.sugg_qty = 1;
                                                                red22.act_qty = 0; red22.item_type = ""; red22.emp_num = "0"; red22.quantity = 1;
                                                                red22.result_msg = red22.po_number + " " + red22.po_suffix; red22.case_qtyitem = 0; red22.box_qtyitem = 0;
                                                                itemqty = 0; red22.cc_type = item44.so.UnitCode;
                                                                red22.extdcost = item44.so.SettingMin - amountofcal; red22.oldunitcost = item44.so.SettingMin - amountofcal;
                                                                red22.doc_id = foundNameabrg.NameAbrg; red22.guid = item44.so.SettingMin - amountofcal;
                                                                mylistfinal.Add(red22);
                                                            }
                                                        }
                                                        amountofcal = 0; newcount = 0;
                                                        ponumberfor = itemcal.Remotekey; suffixfor = itemcal.posuffix;

                                                    }
                                                    var newresu = calresult.Where(x => x.Remotekey == itemcal.Remotekey && x.posuffix == itemcal.posuffix).ToList();
                                                    newcount = newresu.Count;
                                                    AudiViewModel red = new AudiViewModel();
                                                    red.substat_code = item44.to.RateDescId;
                                                    red.wh_num = whnum;
                                                    red.co_num = conum; red.abs_num = itemcal.absnum;
                                                    red.transmission = item44.so.SettingId;
                                                    red.oldunitcost = item44.so.SettingMin;
                                                    red.unitcost = item44.so.RateAmount;
                                                    red.fromdatetime = item44.so.ChargeName; red.po_number = itemcal.Remotekey; red.po_suffix = itemcal.posuffix; red.abs_num = "";
                                                    red.po_line = 0; red.lot = ""; red.row_status = ""; red.sugg_qty = itemcal.Total;
                                                    red.act_qty = 0; red.item_type = ""; red.emp_num = "0"; red.quantity = itemcal.Total;
                                                    red.result_msg = red.po_number + " " + red.po_suffix; red.case_qtyitem = 0; red.box_qtyitem = 0;
                                                    itemqty = 0; red.cc_type = item44.so.UnitCode;
                                                    red.extdcost = item44.so.RateAmount * itemcal.Total;
                                                    red.doc_id = foundNameabrg.NameAbrg;

                                                    if (red.oldunitcost > red.extdcost)
                                                    {
                                                        if (newresu.Count == 1)
                                                            red.guid = item44.so.SettingMin;
                                                    }


                                                    else
                                                    {

                                                        red.guid = item44.so.RateAmount * itemcal.Total;


                                                    }
                                                    if (red.guid > item44.so.SettingMax && item44.so.SettingMax != 0)
                                                    {
                                                        if (newresu.Count == 1)
                                                            red.guid = item44.so.SettingMax;
                                                    }
                                                    if (newresu.Count > 1)
                                                    {
                                                        amountofcal += item44.so.RateAmount * itemcal.Total;
                                                        red.guid = item44.so.RateAmount * itemcal.Total;
                                                    }
                                                    mylistfinal.Add(red);
                                                }

                                            }
                                            else
                                            {
                                                var ponub = ""; var posuf = ""; var uio = 0;
                                                Dictionary<string, string> dict = new Dictionary<string, string>();
                                                foreach (var item66 in lastmylist2)
                                                {
                                                    if (search.CalculRate == 30)
                                                    {
                                                        var findco = (from c in db1.PoCloseRequireds
                                                                      where c.Poconum == conum && c.Powhnum == whnum
                                                                      && c.Poponumber == item66.Remotekey && c.Posuffix == item66.posuffix
                                                                      && c.PoCloseDescId == item44.to.RateDescId
                                                                      select c).FirstOrDefault();
                                                        
                                                        try
                                                        {
                                                            var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item44.to.RateDescId);
                                                            if (df.ForGroup == 1)
                                                            {
                                                                var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item44.to.RateDescId && x.GroupName.Trim() == empnum.Trim() && x.KindId == inout).FirstOrDefault();
                                                                item44.so.RateAmount = foundrateforGroup.GroupRate;
                                                            }

                                                        }
                                                        catch (Exception)
                                                        {

                                                            if (empnum.Length != 0) { item44.so.RateAmount = 0; item44.so.SettingMin = 0; }
                                                        }
                                                        decimal? qqy = 0;
                                                        if (findco != null) qqy = findco.PoCloseRate;
                                                        
                                                        uio = 0;
                                                        foreach (KeyValuePair<string, string> pair in dict)
                                                        {
                                                            if (pair.Key == item66.Remotekey + "-" + item66.posuffix) { qqy = 0; uio = 1; }

                                                        }
                                                        if (uio == 0)
                                                        {
                                                            var dictt = item66.Remotekey + "-" + item66.posuffix;
                                                            dict.Add(dictt, "1");
                                                        }
                                                        AudiViewModel red = new AudiViewModel();
                                                        red.wh_num = whnum;
                                                        red.co_num = conum;
                                                        red.substat_code = item44.so.RateDescId;
                                                        red.abs_num = item66.absnum;
                                                        red.oldunitcost = item44.so.SettingMin;
                                                        red.transmission = item44.so.SettingId;
                                                        red.unitcost = item44.so.RateAmount;
                                                        red.fromdatetime = item44.so.ChargeName;
                                                        red.po_number = item66.Remotekey;
                                                        red.po_suffix = item66.posuffix;
                                                        red.extdcost = item44.so.RateAmount * qqy;
                                                        red.quantity = qqy;
                                                        red.sugg_qty = qqy;
                                                        red.guid = item44.so.RateAmount * qqy;
                                                        if (red.guid < red.oldunitcost) red.guid = red.oldunitcost;
                                                        red.doc_id = item44.to.RateDescId.ToString();
                                                        red.case_qtyitem = 0;
                                                        red.box_qtyitem = 0;
                                                        red.emp_num = empnum;
                                                        red.doc_id = item44.to.NameAbrg;
                                                        red.result_msg = item66.Remotekey + "-" + item66.posuffix;
                                                        if (red.guid > item44.so.SettingMax && item44.so.SettingMax != 0)
                                                        {
                                                            red.guid = item44.so.SettingMax;
                                                        }
                                                        if (red.guid < item44.so.SettingMin && item44.so.SettingMin != 0)
                                                        {
                                                            red.guid = item44.so.SettingMin;
                                                        }
                                                        mylistfinal.Add(red);

                                                    }
                                                    else
                                                    {
                                                        forflat += 1;
                                                        newtotal = item66.Total;

                                                        if (search.RatebyName.Trim() == "Unique Pallet ID" && inout == 1)
                                                        {
                                                            palletidlist.Clear();

                                                            var findabs588 = db.auditlogs.Where(x => x.abs_num.Trim().ToUpper() == item66.absnum.Trim().ToUpper() && x.co_num == conum && x.wh_num == whnum && x.po_number == item66.Remotekey && x.po_suffix == item66.posuffix && x.trans_type == "RE").GroupBy(x => new { x.po_number, x.po_suffix, x.pallet_id }).Select(x => new
                                                            {
                                                                Remotekey = x.Key.po_number,
                                                                posuffix = x.Key.po_suffix,

                                                            }).OrderBy(x => x.Remotekey).ToList();

                                                            newtotal = Convert.ToDecimal(findabs588.Count);
                                                            

                                                        }
                                                        var totalqtyy = newtotal;
                                                        item44.so.RateAmount = wamm.RateAmount;
                                                        if (wponumber != item66.Remotekey || wposuffix != item66.posuffix)
                                                        {
                                                            palletidlist.Clear();
                                                        }
                                                        if (calzala == 0)
                                                        {
                                                            wponumber = item66.Remotekey;
                                                            wposuffix = item66.posuffix;
                                                            empnum = item66.empnum;
                                                            if (empnum == null) empnum = "";
                                                            calzala = 1;
                                                        }

                                                        if (wponumber != item66.Remotekey || wposuffix != item66.posuffix || empnum != item66.empnum)
                                                        {
                                                            wponumber = item66.Remotekey;
                                                            wposuffix = item66.posuffix;
                                                            empnum = item66.empnum;
                                                            if (empnum == null) empnum = "";
                                                            calzala = 1;
                                                        }


                                                        newquantity2 = newtotal;

                                                        var newcal = db1.Ratebys.Where(x => x.RatebyId == item44.so.RatebyId).FirstOrDefault();
                                                        if (newcal.CalculRate == 44)
                                                        {
                                                            if (forflat == 1)
                                                            {
                                                                newquantity2 = 1;
                                                                newtotal = 1;
                                                            }
                                                            else
                                                            {
                                                                newquantity2 = 0;
                                                                newtotal = 0;
                                                            }

                                                        }
                                                        if (inout == 3)
                                                        {
                                                            if (newcal != null)
                                                            {
                                                                if (newcal.CalculRate == 12)
                                                                {
                                                                    newquantity2 = 1;
                                                                    newtotal = 1;
                                                                }

                                                            }
                                                        }
                                                        if (newcal != null)
                                                        {
                                                            decimal? totalqty = 0;
                                                            if (newcal.PalletId == 1)
                                                            {

                                                                calzala += 1;

                                                                if (inout != 3)
                                                                {
                                                                    var withoutgroup = mylistpaletid.Where(c => c.co_num == conum && c.wh_num == whnum && c.po_number == item66.Remotekey && c.po_suffix == item66.posuffix
                                                                                        && c.abs_num.Trim().ToUpper() == item66.absnum.Trim().ToUpper() && c.trans_type == "RE")
                                                                                        .ToList();

                                                                    foreach (var item999 in takeoff)
                                                                    {

                                                                        withoutgroup.RemoveAll(item7 => item7.abs_num.ToUpper() == item999.AbsnumName.ToUpper());


                                                                    }
                                                                    var lastgroup = withoutgroup.Where(x => x.sugg_qty != 0 && x.abs_num.Trim().ToUpper() == item66.absnum.Trim().ToUpper()).GroupBy(x => new { x.pallet_id }).Select(x => new
                                                                    {

                                                                        palettid = x.Key.pallet_id,
                                                                        Total9 = x.Sum(xx => xx.quantity)

                                                                    }).ToList();
                                                                    newtotal = 0;
                                                                    foreach (var calculpalet in lastgroup)
                                                                    {
                                                                        if (palletidlist.Contains(calculpalet.palettid))
                                                                        {
                                                                        }
                                                                        else
                                                                        {
                                                                            palletidlist.Add(calculpalet.palettid);
                                                                            newtotal += 1;
                                                                            newquantity2 = newtotal;
                                                                        }
                                                                    }
                                                                }
                                                                else
                                                                {

                                                                    var lastmylist250 = mylistfinal2.Where(x => x.abs_num.ToUpper() == item66.absnum.ToUpper()).FirstOrDefault();
                                                                    if (inout == 3 && newcal.CalculRate == 2)
                                                                    {
                                                                        var mylis = (from c in result
                                                                                     where c.abs_num.ToUpper() == item66.absnum.ToUpper()
                                                                                     group c by new { c.pallet_id } into d

                                                                                     select new
                                                                                     {
                                                                                         Remotekey = d.Key.pallet_id,
                                                                                         totqt = d.Sum(x => x.quantity)

                                                                                     }).ToList();
                                                                        if (mylis.Count == 0)
                                                                        {
                                                                            newtotal = 0;
                                                                            newquantity2 = 0;
                                                                            totalqty = 0;
                                                                        }
                                                                        else
                                                                        {
                                                                            newtotal = mylis.Count;
                                                                            newquantity2 = mylis.Count;
                                                                            totalqty = mylis.Sum(x => x.totqt);
                                                                        }
                                                                    }
                                                                    else
                                                                    {
                                                                        totalqty = newquantity2;
                                                                        Tuple<decimal?> myswitch = CalculHandling.CalculRateFore2(newcal.CalculRate, newquantity2, newquantity2, lastmylist250.item_qty, lastmylist250.act_qty, lastmylist250.case_qtyitem, lastmylist250.box_qtyitem);
                                                                        newtotal = myswitch.Item1;
                                                                        newquantity2 = newtotal;
                                                                    }
                                                                }


                                                            }
                                                            else
                                                            {


                                                                try
                                                                {
                                                                    foreach (var itemmylist in mylistfinal2)
                                                                    {
                                                                        if (itemmylist.doc_id == null) itemmylist.doc_id = "";
                                                                        if (itemmylist.abs_num == null) itemmylist.abs_num = "";
                                                                        if (itemmylist.bin_to == null) itemmylist.bin_to = "";
                                                                    }
                                                                    var lastmylist25 = mylistfinal2.Where(x => x.quantity != 0 && x.abs_num.ToUpper() == item66.absnum.ToUpper() && x.doc_id != "S" && x.po_number == item66.Remotekey && x.po_suffix == item66.posuffix && x.bin_to == "x").GroupBy(x => new { x.po_number, x.po_suffix, x.abs_num, x.emp_num }).Select(x => new
                                                                    {
                                                                        Remotekey = x.Key.po_number,
                                                                        posuffix = x.Key.po_suffix,
                                                                        absnum = x.Key.abs_num,
                                                                        empnum = x.Key.emp_num,
                                                                        Total = x.Sum(xx => xx.quantity)

                                                                    }).FirstOrDefault();
                                                                    decimal? newtotal1 = newtotal;
                                                                    if (lastmylist25.Total != 0)
                                                                    {
                                                                        newtotal1 = lastmylist25.Total;
                                                                    }

                                                                    var lastmylist250 = mylistfinal2.Where(x => x.abs_num.ToUpper() == item66.absnum.ToUpper()).FirstOrDefault();

                                                                    
                                                                    totalqty = newtotal1;
                                                                    Tuple<decimal?> myswitch = CalculHandling.CalculRateFore2(newcal.CalculRate, newtotal1, lastmylist25.Total, lastmylist250.item_qty, lastmylist250.act_qty, lastmylist250.case_qtyitem, lastmylist250.box_qtyitem);
                                                                    newtotal = myswitch.Item1;



                                                                    
                                                                    newquantity2 = newtotal;
                                                                    if (newcal.CalculRate == 44)
                                                                    {
                                                                        if (forflat == 1)
                                                                        {
                                                                            newtotal = 1;
                                                                            newquantity2 = 1;

                                                                        }
                                                                        else
                                                                        {
                                                                            newtotal = 0;
                                                                            newquantity2 = 0;
                                                                        }
                                                                    }
                                                                }
                                                                catch (Exception ex)
                                                                {
                                                                    var h = ex.Message;
                                                                    newtotal = 0;
                                                                    newquantity2 = 0;
                                                                }

                                                            }
                                                            try
                                                            {
                                                                if (newcal.CalculRate != 44)
                                                                {
                                                                    var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item44.to.RateDescId);
                                                                    if (df.ForGroup == 1)
                                                                    {
                                                                        var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item44.to.RateDescId && x.GroupName.Trim() == empnum.Trim() && x.KindId == inout).FirstOrDefault();
                                                                        item44.so.RateAmount = foundrateforGroup.GroupRate;
                                                                    }
                                                                }
                                                            }
                                                            catch (Exception)
                                                            {

                                                                if (empnum.Length != 0) {
                                                                    item44.so.RateAmount = 0;
                                                                    
                                                                }
                                                            }

                                                            AudiViewModel red = new AudiViewModel();
                                                            red.wh_num = whnum;
                                                            red.co_num = conum;
                                                            red.substat_code = item44.so.RateDescId;
                                                            red.abs_num = item66.absnum;
                                                            red.oldunitcost = item44.so.SettingMin;
                                                            red.transmission = item44.so.SettingId;
                                                            red.unitcost = item44.so.RateAmount;
                                                            red.fromdatetime = item44.so.ChargeName;
                                                            red.po_number = item66.Remotekey;
                                                            red.po_suffix = item66.posuffix;
                                                            red.extdcost = item44.so.RateAmount * newquantity2;
                                                            red.quantity = newtotal;
                                                            red.sugg_qty = item66.Total;
                                                            red.guid = item44.so.RateAmount * newquantity2;
                                                            if (red.guid < red.oldunitcost) red.guid = red.oldunitcost;
                                                            red.doc_id = item44.to.RateDescId.ToString();
                                                            red.case_qtyitem = 0;
                                                            red.box_qtyitem = 0;
                                                            red.emp_num = empnum;
                                                            red.doc_id = item44.to.NameAbrg;
                                                            red.result_msg = item66.Remotekey + "-" + item66.posuffix;
                                                            if (red.guid > item44.so.SettingMax && item44.so.SettingMax != 0)
                                                            {
                                                                red.guid = item44.so.SettingMax;
                                                            }
                                                            if (red.guid < item44.so.SettingMin && item44.so.SettingMin != 0)
                                                            {
                                                                red.guid = item44.so.SettingMin;
                                                            }
                                                            mylistfinal.Add(red);
                                                        }

                                                    }

                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if (item44.to.TypeCalculate == 26 && search.CalculRate == 30)
                            {

                                if (search.CalculRate != 31)
                                {
                                    var lastmylist2 = mylistfinal2.GroupBy(x => new { x.po_number, x.po_suffix }).Select(x => new
                                    {
                                        Remotekey = x.Key.po_number,
                                        posuffix = x.Key.po_suffix,
                                    }).OrderBy(x => x.Remotekey).ToList();
                                    foreach (var itembeling in lastmylist2)
                                    {

                                        
                                        if (inout == 2)
                                        {
                                            var findpo = db.ordhdrs.Where(x => x.order_ == itembeling.Remotekey && x.order_suffix == itembeling.posuffix).ToList();
                                            if (findpo.Count != 0)
                                            {
                                                var secondfindpo = findpo.FirstOrDefault();
                                                var resultpoone = db9.GetAll().Where(x => x.PoCloseid == secondfindpo.id).ToList();
                                                if (resultpoone.Count > 1)
                                                {
                                                    var resultpo = db9.GetAll().Where(x => x.PoCloseid == secondfindpo.id && x.PoCloseChargeId == 9).ToList();
                                                    if (resultpo.Count != 0)
                                                    {
                                                        var secondresultpo = resultpo.FirstOrDefault();
                                                        AudiViewModel red = new AudiViewModel();

                                                        red.extdcost = item44.so.RateAmount;
                                                        red.wh_num = whnum;
                                                        red.co_num = conum;
                                                        red.substat_code = item44.so.RateDescId;
                                                        red.transmission = item44.so.SettingId;
                                                        red.action_code = item44.so.UnitCode;
                                                        red.cc_type = item44.so.UnitCode;
                                                        red.oldunitcost = item44.so.SettingMin;
                                                        red.unitcost = item44.so.RateAmount;
                                                        red.fromdatetime = item44.so.ChargeName;
                                                        red.po_number = itembeling.Remotekey;
                                                        red.po_suffix = itembeling.posuffix;
                                                        red.quantity = secondresultpo.PoCloseRate;
                                                        red.sugg_qty = secondresultpo.PoCloseRate;
                                                        if (secondresultpo.PoCloseRate != 0)
                                                        {
                                                            if (item44.so.UnitCode.ToUpper() == "HR" || item44.so.UnitCode.ToUpper() == "HHR")
                                                            {
                                                                if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                red.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red.quantity);
                                                            }
                                                        }
                                                        red.guid = item44.so.RateAmount * red.quantity;
                                                        if (item44.so.SettingMax > 0)
                                                        {
                                                            if (red.guid > item44.so.SettingMax)
                                                            {
                                                                red.guid = item44.so.SettingMax;
                                                            }
                                                        }
                                                        if (item44.so.SettingMin > 0)
                                                        {

                                                            if (red.guid < item44.so.SettingMin)
                                                            {
                                                                red.guid = item44.so.SettingMin;
                                                            }
                                                        }
                                                        red.case_qtyitem = 0;
                                                        red.box_qtyitem = 0;
                                                        mylistfinal.Add(red);

                                                    }
                                                }
                                                else
                                                {
                                                    if (resultpoone.Count != 0)
                                                    {
                                                        var secondresultpo = resultpoone.FirstOrDefault();
                                                        AudiViewModel red = new AudiViewModel();

                                                        red.extdcost = item44.so.RateAmount;
                                                        red.wh_num = whnum;
                                                        red.co_num = conum;
                                                        red.substat_code = item44.so.RateDescId;
                                                        red.transmission = item44.so.SettingId;
                                                        red.action_code = item44.so.UnitCode;
                                                        red.cc_type = item44.so.UnitCode;
                                                        red.oldunitcost = item44.so.SettingMin;
                                                        red.unitcost = item44.so.RateAmount;
                                                        red.fromdatetime = item44.so.ChargeName;
                                                        red.po_number = itembeling.Remotekey;
                                                        red.po_suffix = itembeling.posuffix;
                                                        red.quantity = secondresultpo.PoCloseRate;
                                                        red.sugg_qty = secondresultpo.PoCloseRate;
                                                        if (secondresultpo.PoCloseRate != 0)
                                                        {
                                                            if (item44.so.UnitCode.ToUpper() == "HR" || item44.so.UnitCode.ToUpper() == "HHR")
                                                            {
                                                                if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                                if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                                if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                                hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                                red.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red.quantity);
                                                            }
                                                        }
                                                        red.guid = item44.so.RateAmount * red.quantity;
                                                        if (item44.so.SettingMax > 0)
                                                        {
                                                            if (red.guid > item44.so.SettingMax)
                                                            {
                                                                red.guid = item44.so.SettingMax;
                                                            }
                                                        }
                                                        if (item44.so.SettingMin > 0)
                                                        {

                                                            if (red.guid < item44.so.SettingMin)
                                                            {
                                                                red.guid = item44.so.SettingMin;
                                                            }
                                                        }
                                                        red.case_qtyitem = 0;
                                                        red.box_qtyitem = 0;
                                                        mylistfinal.Add(red);

                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if (item44.to.TypeCalculate == 26 && search.CalculRate != 30)
                            {
                                var lastmylist2 = mylistfinal2.Where(x => x.quantity != 0 && x.abs_num != null && x.doc_id != "S").GroupBy(x => new { x.po_number, x.po_suffix, x.abs_num, x.emp_num }).Select(x => new
                                {
                                    Remotekey = x.Key.po_number,
                                    posuffix = x.Key.po_suffix,
                                    absnum = x.Key.abs_num,
                                    empnum = x.Key.emp_num,
                                    Total = x.Sum(xx => xx.quantity)

                                }).OrderBy(x => x.Remotekey).ToList();
                                if (search.CalculRate != 31)
                                {
                                    if (search.CalculRate == 12)
                                    {
                                        lastmylist2 = mylistfinal2.Where(x => x.quantity != 0 && x.abs_num != null && x.doc_id != "S").GroupBy(x => new { x.po_number, x.po_suffix, x.abs_num, x.emp_num, x.bin_num }).Select(x => new
                                        {
                                            Remotekey = x.Key.po_number,
                                            posuffix = x.Key.po_suffix,
                                            absnum = x.Key.abs_num,
                                            empnum = x.Key.emp_num,
                                            Total = x.Sum(xx => xx.quantity)

                                        }).OrderBy(x => x.Remotekey).ToList();
                                    }
                                    if (item44.so.SplitQty != 0)
                                    {
                                        palletidlist.Clear();
                                        var lastmylist22 = mylistfinal2.Where(x => x.quantity != 0 && x.abs_num != null).GroupBy(x => new { x.po_number, x.po_suffix, x.abs_num, x.emp_num, x.date_time2 }).Select(x => new
                                        {
                                            Remotekey = x.Key.po_number,
                                            posuffix = x.Key.po_suffix,
                                            absnum = x.Key.abs_num,
                                            empnum = x.Key.emp_num,
                                            datetime2 = x.Key.date_time2,
                                            Total = x.Sum(xx => xx.quantity)

                                        }).ToList();
                                        foreach (var item66 in lastmylist22)
                                        {
                                            DateTime wdatt = DateTime.Now;

                                            var lastmylist222 = mylistfinal2.FirstOrDefault(x => x.po_number == item66.Remotekey && x.po_suffix == item66.posuffix && x.trans_type.ToUpper() == "RC");
                                            if (lastmylist222 != null)
                                            {
                                                var yyear = Convert.ToInt32(lastmylist222.date_time.Substring(0, 4));
                                                var mmonth = Convert.ToInt32(lastmylist222.date_time.Substring(4, 2));
                                                var dday = Convert.ToInt32(lastmylist222.date_time.Substring(6, 2));
                                                wdatt = new DateTime(yyear, mmonth, dday);


                                            }
                                            newtotal = item66.Total;
                                            if (item66.empnum.Trim().ToUpper() == "CONT") newtotal = 0;
                                            if (item66.absnum == "Container") newtotal = 0;
                                            if (calzala == 0)
                                            {
                                                wponumber = item66.Remotekey;
                                                wposuffix = item66.posuffix;
                                                empnum = item66.empnum;
                                                if (empnum == null) empnum = "";
                                                calzala = 1;
                                            }
                                            if (wponumber != item66.Remotekey || wposuffix != item66.posuffix)
                                            {
                                                palletidlist.Clear();
                                            }
                                            if (wponumber != item66.Remotekey || wposuffix != item66.posuffix || empnum != item66.empnum)
                                            {
                                                wponumber = item66.Remotekey;
                                                wposuffix = item66.posuffix;
                                                empnum = item66.empnum;
                                                if (empnum == null) empnum = "";
                                                calzala = 1;
                                            }


                                            newquantity2 = newtotal;
                                            decimal? newRateamount = 0;
                                            foreach (var item in rt1)
                                            {

                                                item.so.RateAmount = wamm.RateAmount;
                                                newRateamount = item.so.RateAmount;
                                                try
                                                {
                                                    var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item.to.RateDescId);
                                                    if (df.ForGroup == 1)
                                                    {
                                                        var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item.to.RateDescId && x.GroupName.Trim() == empnum.Trim() && x.KindId == inout).FirstOrDefault();
                                                        item.so.RateAmount = foundrateforGroup.GroupRate;
                                                    }
                                                }
                                                catch (Exception)
                                                {
                                                    if (empnum.Length != 0) { item.so.RateAmount = 0; item.so.SettingMin = 0; }

                                                }
                                                try
                                                {
                                                    if (item44.so.SplitQty < wdatt.Day)
                                                    {
                                                        if (item.to.TypeCalculate == 39 || item.to.TypeCalculate == 50 || item.to.TypeCalculate == 51)
                                                        {
                                                            if (wdatt.Day >= 16) newRateamount = newRateamount / 2;
                                                        }
                                                    }
                                                }
                                                catch (Exception)
                                                {

                                                    newRateamount = item.so.RateAmount;
                                                }

                                                var newcal = db1.Ratebys.Where(x => x.RatebyId == item.so.RatebyId).FirstOrDefault();
                                                if (newcal.PalletId == 1)
                                                {

                                                    calzala += 1;
                                                    var withoutgroup = mylistpaletid.Where(c => c.co_num == conum && c.wh_num == whnum && c.po_number == item66.Remotekey && c.po_suffix == item66.posuffix
                                                                        && c.abs_num.ToUpper() == item66.absnum.ToUpper() && c.trans_type == "RE")
                                                                        .ToList();

                                                    foreach (var item999 in takeoff)
                                                    {

                                                        withoutgroup.RemoveAll(item7 => item7.abs_num.ToUpper() == item999.AbsnumName.ToUpper());


                                                    }
                                                    var lastgroup = withoutgroup.Where(x => x.quantity != 0 && x.abs_num != null).GroupBy(x => new { x.pallet_id }).Select(x => new
                                                    {

                                                        palettid = x.Key.pallet_id,
                                                        Total9 = x.Sum(xx => xx.quantity)

                                                    }).ToList();

                                                    newtotal = 0;
                                                    foreach (var calculpalet in lastgroup)
                                                    {
                                                        if (palletidlist.Contains(calculpalet.palettid))
                                                        {
                                                        }
                                                        else
                                                        {
                                                            palletidlist.Add(calculpalet.palettid);
                                                            newtotal += 1;
                                                            newquantity2 = newtotal;
                                                        }
                                                    }


                                                    // newquantity2 = lastgroup.Count();
                                                    //  if (calzala > 2) { newquantity2 = 0; }
                                                    newtotal = newquantity2;


                                                }
                                                try
                                                {
                                                    var df = db1.RateDescs.FirstOrDefault(x => x.RateDescId == item.to.RateDescId);
                                                    if (df.ForGroup == 1)
                                                    {
                                                        var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item.to.RateDescId && x.GroupName.Trim() == empnum.Trim() && x.KindId == inout).FirstOrDefault();
                                                        item.so.RateAmount = foundrateforGroup.GroupRate;
                                                    }
                                                }
                                                catch (Exception)
                                                {
                                                    if (empnum.Length != 0) { item.so.RateAmount = 0; item.so.SettingMin = 0; }

                                                }
                                                var lastmylist250 = db.items.FirstOrDefault(x => x.abs_num.ToUpper() == item66.absnum.ToUpper() && x.wh_num == whnum);
                                                Tuple<decimal?> myswitch = CalculHandling.CalculRateFore2(newcal.CalculRate, newtotal, newtotal, lastmylist250.pallet_qty, lastmylist250.weight, lastmylist250.case_qty, lastmylist250.box_qty);
                                                newtotal = myswitch.Item1;
                                                if (newquantity2 != 0)
                                                {
                                                    if (item.so.UnitCode.ToUpper() == "HR" || item.so.UnitCode.ToUpper() == "HHR")
                                                    {
                                                        if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                                        if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                                        if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                                        hourlymin = foundsettingcustomer.SettingBaseNew3;
                                                        newquantity2 = Getquantity("HR", hincrem, hourlymin, (decimal)newquantity2);
                                                    }
                                                }
                                                AudiViewModel red = new AudiViewModel();
                                                red.wh_num = whnum;
                                                red.co_num = conum;
                                                red.substat_code = item.so.RateDescId;
                                                red.abs_num = item66.absnum;
                                                red.transmission = item.so.SettingId;
                                                red.oldunitcost = item.so.SettingMin;
                                                red.unitcost = newRateamount;
                                                red.fromdatetime = item.so.ChargeName;
                                                red.po_number = item66.Remotekey;
                                                red.po_suffix = item66.posuffix;
                                                red.extdcost = newRateamount * newquantity2;
                                                red.quantity = newtotal;
                                                red.sugg_qty = newtotal;
                                                red.guid = newRateamount * newquantity2;
                                                red.doc_id = item.to.RateDescId.ToString();
                                                red.case_qtyitem = 0;
                                                red.box_qtyitem = 0;
                                                red.emp_num = empnum;
                                                red.doc_id = item.to.NameAbrg;
                                                red.result_msg = item66.Remotekey + "-" + item66.posuffix;
                                                if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                                                {
                                                    red.guid = item.so.SettingMax;
                                                }
                                                mylistfinal.Add(red);


                                            }


                                        }

                                    }
                                    else
                                    {

                                        palletidlist.Clear();
                                        foreach (var item66 in lastmylist2)
                                        {
                                            forflat += 1;
                                            newtotal = item66.Total;
                                            item44.so.RateAmount = wamm.RateAmount;
                                            if (wponumber != item66.Remotekey || wposuffix != item66.posuffix)
                                            {
                                                palletidlist.Clear();
                                            }
                                            if (calzala == 0)
                                            {
                                                wponumber = item66.Remotekey;
                                                wposuffix = item66.posuffix;
                                                empnum = item66.empnum;
                                                if (empnum == null) empnum = "";
                                                calzala = 1;
                                            }

                                            if (wponumber != item66.Remotekey || wposuffix != item66.posuffix || empnum != item66.empnum)
                                            {
                                                wponumber = item66.Remotekey;
                                                wposuffix = item66.posuffix;
                                                empnum = item66.empnum;
                                                if (empnum == null) empnum = "";
                                                calzala = 1;
                                            }


                                            newquantity2 = newtotal;

                                            var newcal = db1.Ratebys.Where(x => x.RatebyId == item44.so.RatebyId).FirstOrDefault();
                                            if (newcal.CalculRate == 44)
                                            {
                                                if (forflat == 1)
                                                {
                                                    newquantity2 = 1;
                                                    newtotal = 1;
                                                }
                                                else
                                                {
                                                    newquantity2 = 0;
                                                    newtotal = 0;
                                                }

                                            }
                                            
                                            if (newcal != null)
                                            {
                                                if (newcal.PalletId == 1)
                                                {

                                                    calzala += 1;

                                                    if (inout != 3)
                                                    {
                                                        var withoutgroup = mylistpaletid.Where(c => c.co_num == conum && c.wh_num == whnum && c.po_number == item66.Remotekey && c.po_suffix == item66.posuffix
                                                                            && c.abs_num.Trim().ToUpper() == item66.absnum.Trim().ToUpper() && c.trans_type == "RE")
                                                                            .ToList();

                                                        foreach (var item999 in takeoff)
                                                        {

                                                            withoutgroup.RemoveAll(item7 => item7.abs_num.ToUpper() == item999.AbsnumName.ToUpper());


                                                        }
                                                        var lastgroup = withoutgroup.Where(x => x.sugg_qty != 0 && x.abs_num.Trim().ToUpper() == item66.absnum.Trim().ToUpper()).GroupBy(x => new { x.pallet_id }).Select(x => new
                                                        {

                                                            palettid = x.Key.pallet_id,
                                                            Total9 = x.Sum(xx => xx.quantity)

                                                        }).ToList();
                                                        newtotal = 0;
                                                        foreach (var calculpalet in lastgroup)
                                                        {
                                                            if (palletidlist.Contains(calculpalet.palettid))
                                                            {
                                                            }
                                                            else
                                                            {
                                                                palletidlist.Add(calculpalet.palettid);
                                                                newtotal += 1;
                                                                newquantity2 = newtotal;
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {

                                                        var lastmylist250 = mylistfinal2.Where(x => x.abs_num.ToUpper() == item66.absnum.ToUpper()).FirstOrDefault();
                                                        
                                                        
                                                        {
                                                            Tuple<decimal?> myswitch = CalculHandling.CalculRateFore2(newcal.CalculRate, newquantity2, newquantity2, lastmylist250.item_qty, lastmylist250.act_qty, lastmylist250.case_qtyitem, lastmylist250.box_qtyitem);
                                                            newtotal = myswitch.Item1;
                                                            newquantity2 = newtotal;
                                                        }
                                                    }


                                                }
                                                else
                                                {


                                                    try
                                                    {
                                                        foreach (var itemmylist in mylistfinal2)
                                                        {
                                                            if (itemmylist.doc_id == null) itemmylist.doc_id = "";
                                                            if (itemmylist.abs_num == null) itemmylist.abs_num = "";
                                                            if (itemmylist.bin_to == null) itemmylist.bin_to = "";
                                                        }
                                                        var lastmylist25 = mylistfinal2.Where(x => x.quantity != 0 && x.abs_num.ToUpper() == item66.absnum.ToUpper() && x.doc_id != "S" && x.bin_to == "x").GroupBy(x => new { x.po_number, x.po_suffix, x.abs_num, x.emp_num }).Select(x => new
                                                        {
                                                            Remotekey = x.Key.po_number,
                                                            posuffix = x.Key.po_suffix,
                                                            absnum = x.Key.abs_num,
                                                            empnum = x.Key.emp_num,
                                                            Total = x.Sum(xx => xx.quantity)

                                                        }).FirstOrDefault();
                                                        decimal? newtotal1 = newtotal;
                                                        if (lastmylist25.Total != 0)
                                                        {
                                                            newtotal1 = lastmylist25.Total;
                                                        }

                                                        var lastmylist250 = mylistfinal2.Where(x => x.abs_num.ToUpper() == item66.absnum.ToUpper()).FirstOrDefault();

                                                        

                                                        Tuple<decimal?> myswitch = CalculHandling.CalculRateFore2(newcal.CalculRate, newtotal1, lastmylist25.Total, lastmylist250.item_qty, lastmylist250.act_qty, lastmylist250.case_qtyitem, lastmylist250.box_qtyitem);
                                                        newtotal = myswitch.Item1;



                                                        
                                                        newquantity2 = newtotal;
                                                        if (newcal.CalculRate == 44)
                                                        {
                                                            if (forflat == 1)
                                                            {
                                                                newtotal = 1;
                                                                newquantity2 = 1;

                                                            }
                                                            else
                                                            {
                                                                newtotal = 0;
                                                                newquantity2 = 0;
                                                            }
                                                        }
                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        var h = ex.Message;
                                                        newtotal = 0;
                                                        newquantity2 = 0;
                                                    }

                                                }
                                                try
                                                {
                                                    if (item44.to.ForGroup == 1)
                                                    {
                                                        var foundrateforGroup = db1.GroupItems.Where(x => x.Conum == conum && x.Whnum == whnum && x.RateDescId == item44.to.RateDescId && x.GroupName.Trim() == empnum.Trim() && x.KindId == inout).FirstOrDefault();
                                                        item44.so.RateAmount = foundrateforGroup.GroupRate;
                                                    }
                                                }
                                                catch (Exception)
                                                {

                                                    if (empnum.Length != 0)
                                                    { item44.so.RateAmount = 0; item44.so.SettingMin = 0; }


                                                }

                                                AudiViewModel red = new AudiViewModel();
                                                red.wh_num = whnum;
                                                red.co_num = conum;
                                                red.substat_code = item44.so.RateDescId;
                                                red.abs_num = item66.absnum;
                                                red.oldunitcost = item44.so.SettingMin;
                                                red.transmission = item44.so.SettingId;
                                                red.unitcost = item44.so.RateAmount;
                                                red.fromdatetime = item44.so.ChargeName;
                                                red.po_number = item66.Remotekey;
                                                red.po_suffix = item66.posuffix;
                                                red.extdcost = item44.so.RateAmount * newquantity2;
                                                red.quantity = newtotal;
                                                red.sugg_qty = newtotal;
                                                red.guid = item44.so.RateAmount * newquantity2;
                                                if (red.guid < red.oldunitcost) red.guid = red.oldunitcost;
                                                red.doc_id = item44.to.RateDescId.ToString();
                                                red.case_qtyitem = 0;
                                                red.box_qtyitem = 0;
                                                red.emp_num = empnum;
                                                red.doc_id = item44.to.NameAbrg;
                                                red.result_msg = item66.Remotekey + "-" + item66.posuffix;
                                                if (red.guid > item44.so.SettingMax && item44.so.SettingMax != 0)
                                                {
                                                    red.guid = item44.so.SettingMax;
                                                }
                                                mylistfinal.Add(red);
                                            }

                                        }
                                    }
                                }
                            }

                        }
                    }

                }
            }


            //********************************* by RatebyQty = 6
            if (mylistfinal.Count != 0)
            {
                mylist = mylistfinal;
            }
            try9
                db1.Dispose(); db1 = new RateContext();
            }
            catch (Exception)
            {


            }



            var rt12 = db1.SettingRates.Join(db1.RateDescs,
                                           so => new { so.RateDescId },
                                         to => new { to.RateDescId },
                                         (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.RateDescCountPo == 1 && x.to.RatebyQty == 2 && x.so.KindId == inout)
                                        .Select(z => new { z.so, z.to }).ToList();
            var lastmylist12 = mylist.Where(x => x.guid != 0 && (x.doc_id == "H" || x.doc_id == "S") && x.po_number != string.Empty).GroupBy(x => new { x.po_number, x.po_suffix }).Select(x => new
            {
                Remotekey = x.Key.po_number,
                posuffix = x.Key.po_suffix,
                Total = x.Sum(xx => xx.guid)

            }).ToList();
            if (inout == 2)
            {
                lastmylist12 = mylist.Where(x => x.guid != 0 && x.doc_id == "H").GroupBy(x => new { x.po_number, x.po_suffix }).Select(x => new
                {
                    Remotekey = x.Key.po_number,
                    posuffix = x.Key.po_suffix,
                    Total = x.Sum(xx => xx.guid)

                }).ToList();
            }
            
            foreach (var item66 in lastmylist12)
            {

                foreach (var item in rt12)
                {
                    var search = db1.Ratebys.FirstOrDefault(x => x.RatebyId == item.so.RatebyId);
                    if (search.CalculRate != 31)
                    {
                        if (item66.Total < item.so.RateAmount || item66.Total < item.so.SettingMax)
                        {
                            AudiViewModel red = new AudiViewModel();
                            red.substat_code = item.so.RateDescId;
                            if (item.so.RateAmount > item66.Total)
                            {
                                red.extdcost = item.so.RateAmount - item66.Total;
                            }
                            else
                            {
                                red.extdcost = 0;
                            }
                            if (item66.Total > item.so.SettingMax && item.so.SettingMax != 0)
                            {

                                red.extdcost = item.so.SettingMax - item66.Total;
                            }
                            red.wh_num = whnum;
                            red.co_num = conum;
                            red.oldunitcost = item.so.RateAmount;
                            red.unitcost = item.so.RateAmount;
                            red.fromdatetime = item.so.ChargeName;
                            red.action_code = item.so.UnitCode;
                            red.transmission = item.so.SettingId;
                            red.po_number = item66.Remotekey;
                            red.po_suffix = item66.posuffix;
                            red.quantity = item66.Total;
                            red.sugg_qty = 0;
                            red.guid = red.extdcost;
                            red.result_msg = item66.Remotekey + "-" + item66.posuffix;
                            red.case_qtyitem = 0;
                            red.box_qtyitem = 0;
                            red.cc_type = item.so.UnitCode;
                            mylistfinal.Add(red);
                        }
                    }
                }


            }
            //**************************************************************** By Po_number
            if (mylistfinal.Count != 0)
            {
                mylist = mylistfinal;
            }

            var rt1515 = db1.SettingRates.Join(db1.RateDescs,
                                           so => new { so.RateDescId },
                                         to => new { to.RateDescId },
                                         (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.RateDescCountPo == 1 && x.to.RatebyQty == 6 && x.so.KindId == inout)
                                        .Select(z => new { z.so, z.to }).ToList();
            foreach (var item in rt1515)
            {
                var search = db1.Ratebys.FirstOrDefault(x => x.RatebyId == item.so.RatebyId);
                if (search.CalculRate != 31)
                {
                    var wunloadf = (from c in mylist
                                    group c by new { c.po_number, c.po_suffix } into d
                                    select new
                                    {
                                        Remotekey = d.Key.po_number,
                                        posuffix = d.Key.po_suffix,
                                        Total = d.Sum(x => x.sugg_qty),
                                        Total1 = d.Sum(x => x.quantity)

                                    }).ToList();

                    foreach (var wunload in wunloadf)
                    {
                        AudiViewModel red = new AudiViewModel();
                        red.wh_num = whnum;
                        red.co_num = conum;
                        red.substat_code = item.so.RateDescId;
                        red.abs_num = "";
                        red.oldunitcost = item.so.SettingMin;

                        red.unitcost = item.so.RateAmount;
                        red.transmission = item.so.SettingId;
                        red.fromdatetime = item.so.ChargeName;
                        red.po_number = wunload.Remotekey;
                        red.po_suffix = wunload.posuffix;
                        red.action_code = item.so.UnitCode;
                        red.cc_type = item.so.UnitCode;
                        red.sugg_qty = wunload.Total + wunload.Total1;
                        decimal Mnt = Convert.ToDecimal((wunload.Total + wunload.Total1) * Convert.ToDecimal(0.005));
                        decimal? Mnt1 = CalculHandling.TruncDecimal(Mnt, 2) + CalculHandling.TruncDecimal1(Mnt, 2);
                        red.quantity = Math.Round(Convert.ToDecimal(Mnt1));
                        red.guid = item.so.RateAmount * red.quantity;
                        red.doc_id = item.to.RateDescId.ToString();
                        red.extdcost = red.guid;
                        red.case_qtyitem = 0;
                        red.box_qtyitem = 0;
                        red.emp_num = empnum;
                        red.doc_id = item.to.NameAbrg;

                        red.result_msg = wunload.Remotekey + "-" + wunload.posuffix;
                        if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                        {
                            red.guid = item.so.SettingMax;
                        }
                        mylistfinal.Add(red);

                    }

                }

            }
            //**************************************************************
            //********************************* by RatebyQty = 7

            if (mylistfinal.Count != 0)
            {
                mylist = mylistfinal;
            }
            try
            {
                db1.Dispose(); db1 = new RateContext();
            }
            catch (Exception)
            {


            }

            var rt15150 = db1.SettingRates.Join(db1.RateDescs,
                                           so => new { so.RateDescId },
                                         to => new { to.RateDescId },
                                         (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.RateDescCountPo == 1 && x.to.RatebyQty == 7 && x.so.KindId == inout)
                                        .Select(z => new { z.so, z.to }).ToList();
            var forflat15150 = 0;
            foreach (var item in rt15150)
            {
                var search = db1.Ratebys.FirstOrDefault(x => x.RatebyId == item.so.RatebyId);
                if (search.CalculRate != 31)
                {
                    forflat15150 = 0;
                    //if (search.RatebyId != 63)
                    //{
                        var wunloadf = (from c in mylist
                                        group c by new { c.po_number, c.po_suffix } into d
                                        select new
                                        {
                                            Remotekey = d.Key.po_number,
                                            posuffix = d.Key.po_suffix,
                                            Total = 1


                                        }).ToList();

                        foreach (var wunload in wunloadf)
                        {
                            forflat15150 += 1;
                            AudiViewModel red = new AudiViewModel();
                            red.wh_num = whnum;
                            red.co_num = conum;
                            red.substat_code = item.so.RateDescId;
                            red.abs_num = "";
                            red.oldunitcost = item.so.SettingMin;
                            red.action_code = item.so.UnitCode;
                            red.cc_type = item.so.UnitCode;
                            red.transmission = item.so.SettingId;
                            red.unitcost = item.so.RateAmount;
                            red.fromdatetime = item.so.ChargeName;
                            red.po_number = wunload.Remotekey;
                            red.po_suffix = wunload.posuffix;

                            red.sugg_qty = 1;
                            red.quantity = 1;
                            if (search.CalculRate == 30 || search.CalculRate == 97)
                            {
                                var findco = (from c in db1.PoCloseRequireds
                                              where c.Poconum == conum && c.Powhnum == whnum
                                              && c.Poponumber == wunload.Remotekey && c.Posuffix == wunload.posuffix
                                              && c.PoCloseDescId == item.to.RateDescId
                                              select c).FirstOrDefault();
                                if (findco != null)
                                {
                                    red.quantity = findco.PoCloseRate; red.sugg_qty = findco.PoCloseRate;
                                }
                                else
                                {
                                    red.quantity = 0; red.sugg_qty = 0;
                                }
                            }
                            var rateb = db1.Ratebys.FirstOrDefault(x => x.RatebyId == item.so.RatebyId);
                            if (rateb.CalculRate == 44 && rateb.RatebyName != "Flat")
                            {
                                if (forflat15150 == 1)
                                {
                                    red.quantity = 1;
                                }
                                else
                                {
                                    red.quantity = 0;
                                }
                            }
                            if (item.so.UnitCode.ToUpper() == "HR" || item.so.UnitCode.ToUpper() == "HHR")
                            {
                                if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                hourlymin = foundsettingcustomer.SettingBaseNew3;
                                red.quantity = Getquantity("HR", hincrem, hourlymin, (decimal)red.quantity);
                            }
                            red.guid = item.so.RateAmount * red.quantity;
                            red.doc_id = item.to.RateDescId.ToString();
                            red.extdcost = red.guid;
                            red.case_qtyitem = 0;
                            red.box_qtyitem = 0;
                            red.emp_num = empnum;
                            red.doc_id = item.to.NameAbrg;
                             red.result_msg = wunload.Remotekey + "-" + wunload.posuffix;
                            var findclaculrate = db1.Ratebys.Where(x => x.RatebyId == item.so.RatebyId).FirstOrDefault();
                            if (findclaculrate.CalculRate == 30 || findclaculrate.CalculRate == 95)
                            {
                                var findco = (from c in db1.PoCloseRequireds
                                              where c.Poconum == conum && c.Powhnum == whnum
                                              && c.Poponumber == red.po_number && c.Posuffix == red.po_suffix
                                              && c.PoCloseDescId == item.to.RateDescId
                                              select c).FirstOrDefault();
                                try
                                {
                                    if (item.so.UnitCode.ToUpper() == "HR" || item.so.UnitCode.ToUpper() == "HHR")
                                    {
                                        if (foundsettingcustomer.SettingBaseNew1 == 1) hincrem = 15;
                                        if (foundsettingcustomer.SettingBaseNew1 == 2) hincrem = 30;
                                        if (foundsettingcustomer.SettingBaseNew1 == 3) hincrem = 1;
                                        hourlymin = foundsettingcustomer.SettingBaseNew3;
                                        findco.PoCloseRate = Getquantity("HR", hincrem, hourlymin, (decimal)findco.PoCloseRate);
                                    }
                                    red.guid = item.so.RateAmount * findco.PoCloseRate;
                                    red.quantity = findco.PoCloseRate;
                                }
                                catch (Exception)
                                {

                                    red.guid = 0;
                                    red.extdcost = 0;
                                    red.quantity = 0;
                                }
                            }
                            if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                            {
                                red.guid = item.so.SettingMax;
                            }
                            if (red.guid < item.so.SettingMin && item.so.SettingMin != 0)
                            {
                                red.guid = item.so.SettingMin;
                            }

                            if (inout == 2 && rateb.CalculRate == 97)
                            {
                                var checcarr = db.ordhdrs.Where(x => x.co_num == conum && x.wh_num == whnum && x.order_ == red.po_number && x.order_suffix == red.po_suffix).FirstOrDefault();
                                if (checcarr != null)
                                {
                                    if (checcarr.carrier.ToUpper() == "FEPL" || checcarr.carrier.ToUpper() == "USPS" || checcarr.carrier.ToUpper() == "UPS")
                                    {
                                        red.sugg_qty = 0;
                                        red.quantity = 0; red.guid = 0;
                                    }
                                }
                            }



                                mylistfinal.Add(red);

                        }
       


                }

            }
            //**************************************************************
            //**************************************************************** By invoice

            if (mylistfinal.Count != 0)
            {
                mylist = mylistfinal;
            }
            foreach (var item in mylist)
            {
                if (item.fromdatetime == "9")
                {
                    var x = "x";
                }
            }
            var rt1220 = db1.SettingRates.Join(db1.RateDescs,
                                           so => new { so.RateDescId },
                                         to => new { to.RateDescId },
                                         (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.RateDescInvoice == 1 && x.to.RatebyQty == 5 && x.so.KindId == inout)
                                        .Select(z => new { z.so, z.to }).ToList();



            foreach (var item in rt1220)
            {
                var search = db1.Ratebys.FirstOrDefault(x => x.RatebyId == item.so.RatebyId);
                if (search.CalculRate != 31)
                {
                    var lastmylist120 = mylist.Where(x => x.doc_id == "S").Sum(x => x.extdcost);
                    if (inout == 3) lastmylist120 = mylist.Where(x => x.doc_id != "D").Sum(x => x.guid);

                    AudiViewModel red = new AudiViewModel();
                    decimal? mnt = item.so.RateAmount;
                    if (lastmylist120.Value != 0) mnt = item.so.RateAmount - lastmylist120.Value;
                    red.extdcost = mnt > 0 ? mnt : 0;
                    red.wh_num = whnum;
                    red.co_num = conum;
                    red.substat_code = item.so.RateDescId;
                    red.oldunitcost = item.so.SettingMin;
                    red.unitcost = item.so.RateAmount;
                    red.fromdatetime = item.so.ChargeName;
                    red.action_code = item.so.UnitCode;
                    red.transmission = item.so.SettingId;
                    red.cc_type = item.so.UnitCode;
                    red.po_number = "";
                    red.quantity = lastmylist120.Value != 0 ? lastmylist120.Value : 1;
                    red.sugg_qty = 0;
                    red.guid = red.extdcost;
                    red.result_msg = "";
                    red.case_qtyitem = 0;
                    red.box_qtyitem = 0;
                    red.doc_id = "S";
                    if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                    {
                        red.guid = item.so.SettingMax;
                    }
                    mylistfinal.Add(red);
                }
            }

            if (mylistfinal.Count != 0)
            {
                mylist = mylistfinal;
            }

            //****************************************************************

            //************
            //***************
            try
            {
                db1.Dispose(); db1 = new RateContext();
            }
            catch (Exception)
            {


            }
            var rt12200 = db1.SettingRates.Join(db1.RateDescs,
                                          so => new { so.RateDescId },
                                        to => new { to.RateDescId },
                                        (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.RateDescInvoice == 1 && x.to.RatebyQty == 25 && x.so.KindId == inout)
                                       .Select(z => new { z.so, z.to }).ToList();



            foreach (var item in rt12200)
            {
                string xto = "20";
                var search = db1.Ratebys.FirstOrDefault(x => x.RatebyId == item.so.RatebyId);
                if (search.CalculRate != 31)
                {
                    var lastmylist120 = mylist.Where(x => x.fromdatetime == xto.Trim()).ToList();

                    var newqu = lastmylist120.Count();
                    if (newqu < item.so.SettingMin)
                    {


                        AudiViewModel red = new AudiViewModel();
                        red.quantity = item.so.SettingMin - newqu;
                        decimal? mnt = item.so.RateAmount * red.quantity;
                        red.extdcost = mnt > 0 ? mnt : 0;
                        red.wh_num = whnum;
                        red.co_num = conum;
                        red.substat_code = item.so.RateDescId;
                        red.oldunitcost = item.so.SettingMin;
                        red.action_code = item.so.UnitCode;
                        red.transmission = item.so.SettingId;
                        red.cc_type = item.so.UnitCode;
                        red.unitcost = item.so.RateAmount;
                        red.fromdatetime = item.so.ChargeName;
                        red.po_number = "";

                        red.sugg_qty = 0;
                        red.guid = red.extdcost;
                        red.result_msg = "";
                        red.case_qtyitem = 0;
                        red.box_qtyitem = 0;

                        mylistfinal.Add(red);
                    }
                }
            }

            if (mylistfinal.Count != 0)
            {
                mylist = mylistfinal;
            }

            //******************************************************************

            var rt120 = db1.SettingRates.Join(db1.RateDescs,
                                           so => new { so.RateDescId },
                                         to => new { to.RateDescId },
                                         (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.RateDescInvoice == 1 && (x.to.RatebyQty != 5 && x.to.RatebyQty != 7) && x.so.KindId == inout)
                                        .Select(z => new { z.so, z.to }).ToList();



            foreach (var item in rt120)
            {
                var search = db1.Ratebys.FirstOrDefault(x => x.RatebyId == item.so.RatebyId);
                if (search.CalculRate != 31)
                {
                  
                    var lastmylist120 = mylist.Where(x => x.doc_id == "S" && x.quantity != 0).Sum(x => x.guid);
                   
                    //   if (inout == 3) lastmylist120 = mylist.Sum(x => x.guid);
                    if (lastmylist120 != null)
                    {
                        AudiViewModel red = new AudiViewModel();
                        red.extdcost = (item.so.RateAmount * lastmylist120.Value);
                        if (search.CalculRate == 44) red.extdcost = item.so.RateAmount;
                        red.wh_num = whnum;
                        red.co_num = conum;
                        red.substat_code = item.so.RateDescId;
                        red.oldunitcost = item.so.SettingMin;
                        red.action_code = item.so.UnitCode;
                        red.cc_type = item.so.UnitCode;
                        red.transmission = item.so.SettingId;
                        red.unitcost = item.so.RateAmount;
                        red.fromdatetime = item.so.ChargeName;
                        red.po_number = "";
                        red.quantity = lastmylist120.Value; if (search.CalculRate == 44) red.quantity = 1;
                        red.sugg_qty = 0;
                        red.guid = red.extdcost;
                        red.result_msg = "";
                        red.case_qtyitem = 0;
                        red.box_qtyitem = 0;
                        if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                        {
                            red.guid = item.so.SettingMax;
                        }
                        if (red.guid < item.so.SettingMin && item.so.SettingMin != 0)
                        {
                            red.guid = item.so.SettingMin;
                        }
                        mylistfinal.Add(red);
                    }
                }
            }

            //**************************************************************** By Damage
            if (mylistfinal.Count != 0)
            {
                mylist = mylistfinal;
            }

            //   Unloading 
            ///////////////////////////////////////////////////////////

            var un120 = db1.SettingRates.Join(db1.RateDescs,
                                           so => new { so.RateDescId },
                                         to => new { to.RateDescId },
                                         (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.RateDescCountPo == 1 && x.to.RatebyQty == 4 && x.so.KindId == inout)
                                        .Select(z => new { z.so, z.to }).ToList();


            groupPonew2 = (from c in mylist

                           group c by new { c.po_number, c.po_suffix } into d
                           select new
                           {
                               Remotekey = d.Key.po_number,
                               posuffix = d.Key.po_suffix,
                               Total = d.Sum(x => x.quantity)

                           }).OrderBy(x => x.Remotekey).ThenBy(x => x.posuffix).ToList();
            foreach (var item in un120)
            {
                var foundRatecal = db1.Ratebys.Where(x => x.RatebyId == item.so.RatebyId).FirstOrDefault();
                var foundNameabrg = db1.RateDescs.Where(x => x.RateDescId == item.so.RateDescId).FirstOrDefault();
                if (foundRatecal.CalculRate != 31)
                {
                    foreach (var itemgroup in groupPonew2)
                    {
                        if (foundRatecal.CalculRate == 30)
                        {
                            var findco = (from c in db1.PoCloseRequireds
                                          where c.Poconum == conum && c.Powhnum == whnum
                                          && c.Poponumber == itemgroup.Remotekey && c.Posuffix == itemgroup.posuffix
                                          && c.PoCloseDescId == item.to.RateDescId
                                          select c).FirstOrDefault();
                            if (findco != null)
                            {
                                AudiViewModel red = new AudiViewModel();
                                red.extdcost = (item.so.RateAmount * findco.PoCloseRate);
                                red.wh_num = whnum;
                                red.co_num = conum;
                                red.substat_code = item.so.RateDescId;
                                red.oldunitcost = item.so.SettingMin;
                                red.action_code = item.so.UnitCode;
                                red.cc_type = item.so.UnitCode;
                                red.transmission = item.so.SettingId;
                                red.unitcost = item.so.RateAmount;
                                red.fromdatetime = item.so.ChargeName;
                                red.po_number = itemgroup.Remotekey;
                                red.quantity = findco.PoCloseRate;
                                red.sugg_qty = 0;
                                red.guid = red.extdcost;
                                red.result_msg = itemgroup.Remotekey + "-" + itemgroup.posuffix;
                                red.case_qtyitem = 0;
                                red.box_qtyitem = 0;
                                try
                                {
                                    red.guid = item.so.RateAmount * findco.PoCloseRate;
                                    if (item.so.SettingMin > 0)
                                    {
                                        if (red.guid < item.so.SettingMin) red.guid = item.so.SettingMin;
                                    }
                                    if (item.so.SettingMax > 0)
                                    {
                                        if (red.guid > item.so.SettingMax) red.guid = item.so.SettingMax;
                                    }
                                    red.quantity = findco.PoCloseRate;
                                }
                                catch (Exception)
                                {
                                    red.sugg_qty = 0;
                                    red.oldunitcost = 0;
                                    red.guid = 0;
                                    red.extdcost = 0;
                                    red.quantity = 0;
                                }

                                if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                                {
                                    red.guid = item.so.SettingMax;
                                }
                                mylistfinal.Add(red);
                            }

                        }


                    }

                }


            }

            //**************************************************************** By Damage
            if (mylistfinal.Count != 0)
            {
                mylist = mylistfinal;
            }

            ///////////////////////////////////////////////////////////////
            var groupPonewList = (from c in mylist
                                  group c by new { c.po_number, c.po_suffix, c.row_status } into d
                                  select new
                                  {
                                      Remotekey = d.Key.po_number,
                                      Rabrow_status = d.Key.row_status,
                                      posuffix = d.Key.po_suffix,
                                      Total = d.Sum(x => x.quantity)

                                  }).ToList();
            try
            {
                db1.Dispose(); db1 = new RateContext();
            }
            catch (Exception)
            {


            }

            var rt1200 = db1.SettingRates.Join(db1.RateDescs,
                                          so => new { so.RateDescId },
                                        to => new { to.RateDescId },
                                        (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.RateDescCountPo == 1 && x.to.RatebyQty == 3 && x.so.KindId == inout)
                                       .Select(z => new { z.so, z.to }).ToList();

            var listosd5 = mylistinvoice.Where(x => x.trans_type == "RE").ToList();
            foreach (var item in listosd5)
            {
                if (item.row_status == null)
                {
                    item.row_status = "";
                }

            }
              foreach (var item in rt1200)
            {
                var search = db1.Ratebys.FirstOrDefault(x => x.RatebyId == item.so.RatebyId);
                var customersetting8 = db7.GetAll().FirstOrDefault(x => x.SettingBaseWhnum == whnum && x.SettingBaseConum == conum);

                if (search.CalculRate != 31)
                {
                    if (item.to.TypeCalculate != 42)
                    {

                        if (search.CalculRate == 30)
                        {
                            foreach (var itemnew in groupPonew88)
                            {

                                var findco = (from c in db1.PoCloseRequireds
                                              where c.Poconum == conum && c.Powhnum == whnum
                                              && c.Poponumber == itemnew.Remotekey && c.Posuffix == itemnew.posuffix
                                              && c.PoCloseDescId == item.to.RateDescId
                                              select c).FirstOrDefault();
                                AudiViewModel red = new AudiViewModel();
                                red.extdcost = item.so.RateAmount;
                                red.wh_num = whnum;
                                red.co_num = conum;
                                red.substat_code = item.so.RateDescId;
                                red.action_code = item.so.UnitCode;
                                red.oldunitcost = item.so.SettingMin;
                                red.unitcost = item.so.RateAmount;
                                red.fromdatetime = item.so.ChargeName;
                                red.po_number = itemnew.Remotekey;
                                red.cc_type = item.so.UnitCode;
                                red.transmission = item.so.SettingId;
                                red.quantity = 1;
                                red.sugg_qty = 1;
                                red.guid = red.extdcost;

                                red.case_qtyitem = 0;
                                red.box_qtyitem = 0;
                                try
                                {
                                    red.guid = item.so.RateAmount * findco.PoCloseRate;
                                    red.extdcost = item.so.RateAmount * findco.PoCloseRate;
                                    red.quantity = findco.PoCloseRate;
                                    red.sugg_qty = findco.PoCloseRate;
                                    red.po_number = itemnew.Remotekey;
                                    red.po_suffix = itemnew.posuffix;
                                }
                                catch (Exception)
                                {

                                    red.guid = 0;
                                    red.extdcost = 0;
                                    red.quantity = 0;
                                    red.sugg_qty = 0;
                                    red.po_number = "delete";
                                }
                                if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                                {
                                    red.guid = item.so.SettingMax;
                                }
                                if (red.guid < item.so.SettingMin && item.so.SettingMin != 0)
                                {
                                    red.guid = item.so.SettingMin;
                                }
                                mylistfinal.Add(red);
                            }
                        }
                        else if (search.CalculRate == 35)
                        {
                            foreach (var itemnew in groupPonew88)
                            {
                                var returnres = db.pomsts.FirstOrDefault(x => x.po_number == itemnew.Remotekey && x.po_suffix == itemnew.posuffix && x.co_num == conum && x.wh_num == whnum);
                                if (returnres != null)
                                {
                                    if (returnres.type.ToUpper().Trim() == "R" || returnres.type.ToUpper().Trim() == "E")
                                    {

                                        AudiViewModel red = new AudiViewModel();
                                        red.extdcost = item.so.RateAmount;
                                        red.wh_num = whnum;
                                        red.co_num = conum;
                                        red.substat_code = item.so.RateDescId;
                                        red.action_code = item.so.UnitCode;
                                        red.oldunitcost = item.so.SettingMin;
                                        red.unitcost = item.so.RateAmount;
                                        red.fromdatetime = item.so.ChargeName;
                                        red.po_number = itemnew.Remotekey;
                                        red.cc_type = item.so.UnitCode;
                                        red.transmission = item.so.SettingId;
                                        red.quantity = 1;
                                        red.sugg_qty = 1;
                                        red.guid = red.extdcost;

                                        red.case_qtyitem = 0;
                                        red.box_qtyitem = 0;
                                        red.guid = item.so.RateAmount * 1;
                                        red.extdcost = item.so.RateAmount * 1;
                                        red.quantity = 1;
                                        red.sugg_qty = 1;
                                        red.po_number = itemnew.Remotekey;
                                        red.po_suffix = itemnew.posuffix;

                                        if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                                        {
                                            red.guid = item.so.SettingMax;
                                        }
                                        if (red.guid < item.so.SettingMin && item.so.SettingMin != 0)
                                        {
                                            red.guid = item.so.SettingMin;
                                        }
                                        mylistfinal.Add(red);
                                    }
                                }
                            }
                        }
                        else
                        {

                            foreach (var itemnew in groupPonewList)
                            {


                                if (itemnew.Rabrow_status == "Y")
                                {
                                    AudiViewModel red = new AudiViewModel();
                                    red.extdcost = item.so.RateAmount;
                                    red.wh_num = whnum;
                                    red.co_num = conum;
                                    red.substat_code = item.so.RateDescId;
                                    red.action_code = item.so.UnitCode;
                                    red.oldunitcost = item.so.SettingMin;
                                    red.unitcost = item.so.RateAmount;
                                    red.fromdatetime = item.so.ChargeName;
                                    red.po_number = itemnew.Remotekey;
                                    red.cc_type = item.so.UnitCode;
                                    red.transmission = item.so.SettingId;
                                    red.quantity = 1;
                                    red.sugg_qty = 1;
                                    red.guid = red.extdcost;

                                    red.case_qtyitem = 0;
                                    red.box_qtyitem = 0;
                                    if (red.guid > item.so.SettingMax && item.so.SettingMax != 0)
                                    {
                                        red.guid = item.so.SettingMax;
                                    }
                                    if (red.guid < item.so.SettingMin && item.so.SettingMin != 0)
                                    {
                                        red.guid = item.so.SettingMin;
                                    }
                                    mylistfinal.Add(red);

                                }

                            }
                        }
                    }
                    if (item.to.TypeCalculate == 42)
                    {
                        //  var listosd = mylistinvoice.Where(x => x.trans_type == "RE" ).GroupBy(x => new { x.po_number, x.po_suffix }).ToList();
                        var listosd = mylistinvoice.Where(x => x.trans_type == "RE" && (x.row_status.Trim() != "")).GroupBy(x => new { x.po_number, x.po_suffix }).ToList();
                        var ress = groupPonew.GroupBy(x => new { x.Remotekey, x.posuffix }).ToList();
                       // var listosdd = listosd.Where(x => x.Key.po_number == ress.Remotekey && x.Key.po_suffix == ress.posuffix).ToList();
                        foreach (var itemosd in ress)
                        {
                            var listosdd = listosd.FirstOrDefault(x => x.Key.po_number == itemosd.Key.Remotekey && x.Key.po_suffix == itemosd.Key.posuffix);
                            if (listosdd != null)
                            {
                                AudiViewModel red = new AudiViewModel();
                                var t = listosdd.Key.po_number + "-" + listosdd.Key.po_suffix;
                                red.extdcost = item.so.RateAmount;
                                red.wh_num = whnum;
                                red.co_num = conum;
                                red.substat_code = item.so.RateDescId;
                                red.action_code = item.so.UnitCode;
                                red.oldunitcost = item.so.SettingMin;
                                red.unitcost = item.so.RateAmount;
                                red.fromdatetime = item.so.ChargeName;
                                red.cc_type = item.so.UnitCode;
                                red.transmission = item.so.SettingId;
                                red.po_number = listosdd.Key.po_number;
                                red.po_suffix = listosdd.Key.po_suffix;
                                red.quantity = 1;
                                red.sugg_qty = 1;
                                red.guid = red.extdcost;
                                red.case_qtyitem = 0;
                                red.box_qtyitem = 0;
                                if (customersetting8.SettingBasePo == 1 && listosdd.Key.po_number != itemosd.Key.Remotekey && listosdd.Key.po_suffix != itemosd.Key.posuffix)
                                {
                                    red.quantity = 0; red.sugg_qty = 0;
                                }
                                mylistfinal.Add(red);
                            }
                        }


                    }
                }
            }
            //*********************************** calculrate = 9 *************************************************
            try
            {
                db1.Dispose(); db1 = new RateContext();
            }
            catch (Exception)
            {


            }
            var rt11 = db1.SettingRates.Join(db1.Ratebys,
                                           so => new { so.RatebyId },
                                         to => new { to.RatebyId },
                                         (so, to) => new { so, to }).Where(x => x.so.co_num == conum && x.so.wh_num == whnum && x.to.CalculRate == 9 && x.so.KindId == inout)
                                        .Select(z => new { z.so, z.to }).ToList();
            if (inout != 2)
            {
                foreach (var item01 in rt11)
                {
                    var search = db1.Ratebys.FirstOrDefault(x => x.RatebyId == item01.so.RatebyId);
                    if (search.CalculRate != 31)
                    {
                        var searchpalletid = mylistinvoice
                                        .Where(x => x.co_num == conum && x.wh_num == whnum)
                                       .GroupBy(x => new { x.lot, x.po_number, x.po_suffix })
                                        .ToList();
                        foreach (var item45 in searchpalletid)
                        {
                            var final = db1.RateDescs.Where(x => x.RateDescId == item01.so.RateDescId).FirstOrDefault();
                            var returnres = db.pomsts.FirstOrDefault(x => x.po_number == item45.Key.po_number && x.po_suffix == item45.Key.po_suffix && x.co_num == conum && x.wh_num == whnum);
                            AudiViewModel red = new AudiViewModel();
                            red.wh_num = whnum;
                            red.co_num = conum;
                            red.substat_code = item01.so.RateDescId;
                            red.action_code = item01.so.UnitCode;
                            red.cc_type = item01.so.UnitCode;
                            red.oldunitcost = item01.so.SettingMin;
                            red.unitcost = item01.so.RateAmount;
                            red.fromdatetime = item01.so.ChargeName;
                            red.po_number = item45.Key.po_number;
                            red.po_suffix = item45.Key.po_suffix;
                            red.transmission = item01.so.SettingId;
                            red.abs_num = "";
                            red.po_line = 0;
                            red.lot = item45.Key.lot;
                            red.row_status = "";
                            red.sugg_qty = 1;
                            red.act_qty = 1;
                            red.item_type = "";
                            red.emp_num = "";
                            red.result_msg = item45.Key.po_number + "-" + item45.Key.po_suffix;
                            red.case_qtyitem = 0;
                            red.box_qtyitem = 0;
                            red.quantity = 1;
                            if (final.TypeCalculate == 40 && item01.to.CalculRate == 35)
                            {
                                if (returnres.type != "R" || returnres.type != "E")
                                {
                                    red.quantity = 0; red.sugg_qty = 0;
                                    red.act_qty = 0;
                                }
                            }
                            red.extdcost = red.quantity * item01.so.RateAmount;
                            red.guid = red.extdcost;
                            if (red.guid > item01.so.SettingMax && item01.so.SettingMax != 0)
                            {
                                red.guid = item01.so.SettingMax;
                            }
                            mylistfinal.Add(red);
                        }
                    }

                }
            }
            //************************************************************************

            lastrub:
            foreach (var item in mylistfinal)
            {
                if (item.line_sequence == 75 && item.quantity == 0)
                {
                    item.quantity = 888777999;
                }
            }
            var res = mylistfinal.Where(x => x.quantity == 0).ToList();
            foreach (var item in mylistfinal)
            {
                if (item.line_sequence == 75 && item.quantity == 888777999)
                {
                    item.quantity = 0; item.guid = 0;
                }
            }
            foreach (var item in res)
            {
                if (item.release_id != "N")
                    mylistfinal.Remove(item);
            }
            var labellfound = mylistfinal.ToList();
            foreach (var item in labellfound)
            {

                try
                {

                    var tt = item.fromdatetime;
                    int chargeid = Convert.ToInt32(item.fromdatetime);
                    item.elapsedtime = chargeid;
                    var rees = db1.ChargeNames.Where(x => x.ChargeId == chargeid).FirstOrDefault();
                    item.fromdatetime = rees.ChargeNameDesc;

                    var t = item.fromdatetime;
                }
                catch (Exception)
                {

                }

            }
            mylistfinal = labellfound;
            //if (inout == 3)
            //{
            //    foreach (var item in mylistfinal)
            //    {
            //        if (item.quantity != 0 && item.extdcost != null)
            //        {
            //            item.guid = item.quantity * item.extdcost;
            //        }
            //    }
            //}

            result = mylistfinal.Where(x => x.guid != null).OrderBy(x => x.po_number).OrderBy(x => x.abs_num);

            return result;
        }