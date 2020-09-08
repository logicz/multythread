using CMPN;
using CMPN.Models.Security;
using CN.App;
using CN.Utils;
using smcorelib.CMPN.Lists;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Practices.Unity;
using smcorelib.Services.Actors;

namespace CompanyName.Services.Lists
{
	public class AutoListUpdater : IAutoListUpdater
	{
		private static int tickCount = 50;

		private readonly Guid listId;
		private ProjectList list;

		private readonly object locker = new object();
		private readonly object processLocker = new object();
		public bool inProcess;
		private Thread updater;

		private int updatedCount;
		private bool updated;

		private Dictionary<Guid, object> singleUpdateLocks = new Dictionary<Guid, object>();

		public AutoListUpdater(ProjectList list)
		{
			this.list = list;
			listId = list.Id;
		}

		private void StartListUpdate()
		{
			lock (locker)
			{
				if (!inProcess)
				{
					updatedCount = 0;
					updated = false;
					inProcess = true;

          // A custom thread extention with context, including DI container and db connection
					updater = SoleThread.Create(Worker, Principal.Current);

					updater.Start();
				}
			}
		}

		private void Worker(object obj)
		{
			lock (processLocker)
			{
				try
				{
					var service = GetListItemService();

					HashSet<Guid> portfolioProjectIds = GetProjectListPortfolioProvider()
						.GetPortfolioProjects(listId)
						.Select(p => p.Id).ToHashSet();

					ClearList(service);

					AddNewItems(service, portfolioProjectIds);

					lock (locker)
					{
						updated = true;
					}
				}
				finally
				{
					lock (locker)
					{
						inProcess = false;
						Monitor.PulseAll(locker);
					}
				}
			}
		}

		private void ClearList(IProjectListItemService service)
		{
			using (var scope = new TransactionScope())
			{
				service.DeleteItemsByList(listId);
				service.BeginTransactionAndCommit(scope);
			}
		}

		private void AddNewItems(IProjectListItemService service, HashSet<Guid> portfolioProjectIds)
		{
			var order = 0;
			var skip = 0;

			while (true)
			{
				var projectIds = portfolioProjectIds.Skip(tickCount * skip++).Take(tickCount);

				if (projectIds.Count() == 0)
				{
					break;
				}
				using (var scope = new TransactionScope())
				{
					var items = new List<ProjectListItem>();

					foreach (var newProjectId in projectIds)
					{
						items.Add(new ProjectListItem
						{
							ProjectListId = listId,
							ProjectId = newProjectId,
							Order = order++
						});
					}

					service.CreateItems(items);
					service.BeginTransactionAndCommit(scope);

					lock (locker)
					{
						updatedCount += tickCount;
						Monitor.PulseAll(locker);
					}
				}
			}
		}

		public void Update()
		{
			Abort();

			StartListUpdate();
		}

		public void UpdateForChangedProjects(IEnumerable<Guid> projectIds)
		{
			var unity = ServiceContext.Current.Unity;

			var sessionProvider = new SimpleInMemorySessionProvider();
			sessionProvider.SetValue(Principal.PRINCIPAL_KEY, list.Author.Principal);
			unity.RegisterInstance<ISessionProvider>(sessionProvider);
			var personService = unity.Resolve<IPersonService>();
			personService.CurrentPersonId = list.Author.Id.ToString();

			var lockObject = new object();

			lock (singleUpdateLocks)
			{
				foreach(var id in projectIds)
				{
					object oldLock;
					if(singleUpdateLocks.TryGetValue(id, out oldLock))
					{
						lock(oldLock);
					}
					singleUpdateLocks.Add(id, lockObject);
				}
			}

			lock (lockObject)
			{
				using (var scope = new TransactionScope())
				{
					var service = GetListItemService();

					var projectsInPortfolioIds = GetProjectListPortfolioProvider().FilterProjectsByPortfolio(listId, projectIds);

					service.DeleteItemsByProjectIdsAndList(
						new List<Guid>(projectIds.Except(projectsInPortfolioIds)),
						listId);

					service.CreateItems(
						projectsInPortfolioIds.Except(service.GetItemsByProjectsAndList(projectsInPortfolioIds, listId).Select(i => i.ProjectId)),
						listId);

					service.BeginTransactionAndCommit(scope);
				}
			}

			lock (singleUpdateLocks)
			{
				foreach (var id in projectIds)
				{
					singleUpdateLocks.Remove(id);
				}
			}
		}

		private IProjectListItemService GetListItemService()
		{
			return ServiceContext.Current.Unity.Resolve<IProjectListItemService>();
		}

		private IProjectListPortfolioProvider GetProjectListPortfolioProvider()
		{
			return ServiceContext.Current.Unity.Resolve<IProjectListPortfolioProvider>();
		}

		public void WaitForPageUpdated(int page, int pageSize)
		{
			bool isUpdated;
			int count = page * pageSize;
			while (true)
			{
				lock (locker)
				{
					if (!inProcess)
					{
						break;
					}

					isUpdated = updated || page > 0 && updatedCount > count;

					if (!isUpdated)
					{
						Monitor.Wait(locker);
					}
					else
					{
						break;
					}
				}
			}
		}

		public void Abort()
		{
			if (updater != null)
			{
				lock (locker)
				{
					updater.Abort();
					updated = false;
				}
				updater.Join();
				updater = null;
			}
		}

		public void WaitForSingleUpdated(Guid projectId)
		{
			object singleLock;

			lock (singleUpdateLocks)
			{
				if (!singleUpdateLocks.TryGetValue(projectId, out singleLock))
				{
					return;
				}
			}

			lock (singleLock)
			{
				return;
			}
		}
	}
}
