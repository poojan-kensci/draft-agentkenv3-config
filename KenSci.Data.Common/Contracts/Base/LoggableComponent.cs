using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KenSci.Data.Common.Contracts.Base
{
    public class LoggableComponent
    {
        internal static readonly NLog.Logger _logger = NLog.LogManager.GetCurrentClassLogger();

    }
}